/*
 * BlobWorker.actor.cpp
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

#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/BlobWorkerCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/network.h"

#include "flow/actorcompiler.h" // has to be last include

#define BW_DEBUG false
#define BW_REQUEST_DEBUG false

/*
 * The Blob Worker is a stateless role assigned a set of granules by the Blob Manager.
 * It is responsible for managing the change feeds for those granules, and for consuming the mutations from those change
 * feeds and writing them out as files to blob storage.
 */

struct GranuleStartState {
	UID granuleID;
	Version changeFeedStartVersion;
	Version previousDurableVersion;
	Optional<std::pair<KeyRange, UID>> parentGranule;
	bool doSnapshot;
	Optional<GranuleFiles> blobFilesToSnapshot;
	Optional<GranuleFiles> existingFiles;
	Optional<GranuleHistory> history;
};

// FIXME: add global byte limit for pending and buffered deltas
struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	KeyRange keyRange;

	GranuleFiles files;
	Standalone<GranuleDeltas>
	    currentDeltas; // only contain deltas in pendingDeltaVersion + 1 through bufferedDeltaVersion

	uint64_t bytesInNewDeltaFiles = 0;
	uint64_t bufferedDeltaBytes = 0;

	// for client to know when it is safe to read a certain version and from where (check waitForVersion)
	Version bufferedDeltaVersion; // largest delta version in currentDeltas (including empty versions)
	Version pendingDeltaVersion = 0; // largest version in progress writing to s3/fdb
	NotifiedVersion durableDeltaVersion; // largest version persisted in s3/fdb
	NotifiedVersion durableSnapshotVersion; // same as delta vars, except for snapshots
	Version pendingSnapshotVersion = 0;
	Version initialSnapshotVersion = invalidVersion;
	Version knownCommittedVersion;

	int64_t originalEpoch;
	int64_t originalSeqno;
	int64_t continueEpoch;
	int64_t continueSeqno;

	Promise<Void> cancelled;
	Promise<Void> readable;
	Promise<Void> historyLoaded;

	Promise<Void> resumeSnapshot;

	AsyncVar<Reference<ChangeFeedData>> activeCFData;

	AssignBlobRangeRequest originalReq;

	void resume() {
		if (resumeSnapshot.canBeSet()) {
			resumeSnapshot.send(Void());
		}
	}
};

struct GranuleRangeMetadata {
	int64_t lastEpoch;
	int64_t lastSeqno;
	Reference<GranuleMetadata> activeMetadata;

	Future<GranuleStartState> assignFuture;
	Future<Void> fileUpdaterFuture;
	Future<Void> historyLoaderFuture;

	void cancel() {
		if (activeMetadata->cancelled.canBeSet()) {
			activeMetadata->cancelled.send(Void());
		}
		activeMetadata.clear();
		assignFuture.cancel();
		historyLoaderFuture.cancel();
		fileUpdaterFuture.cancel();
	}

	GranuleRangeMetadata() : lastEpoch(0), lastSeqno(0) {}
	GranuleRangeMetadata(int64_t epoch, int64_t seqno, Reference<GranuleMetadata> activeMetadata)
	  : lastEpoch(epoch), lastSeqno(seqno), activeMetadata(activeMetadata) {}
};

// represents a previous version of a granule, and optionally the files that compose it.
struct GranuleHistoryEntry : NonCopyable, ReferenceCounted<GranuleHistoryEntry> {
	KeyRange range;
	UID granuleID;
	Version startVersion; // version of the first snapshot
	Version endVersion; // version of the last delta file

	// load files lazily, and allows for clearing old cold-queried files to save memory
	// FIXME: add memory limit and evictor for old cached files
	Future<GranuleFiles> files;

	// FIXME: do skip pointers with single back-pointer and neighbor pointers
	// Just parent reference for now (assumes no merging)
	Reference<GranuleHistoryEntry> parentGranule;

	GranuleHistoryEntry() : startVersion(invalidVersion), endVersion(invalidVersion) {}
	GranuleHistoryEntry(KeyRange range, UID granuleID, Version startVersion, Version endVersion)
	  : range(range), granuleID(granuleID), startVersion(startVersion), endVersion(endVersion) {}
};

struct BlobWorkerData : NonCopyable, ReferenceCounted<BlobWorkerData> {
	UID id;
	Database db;

	BlobWorkerStats stats;

	PromiseStream<Future<Void>> addActor;

	LocalityData locality;
	int64_t currentManagerEpoch = -1;

	AsyncVar<ReplyPromiseStream<GranuleStatusReply>> currentManagerStatusStream;
	bool statusStreamInitialized = false;

	// FIXME: refactor out the parts of this that are just for interacting with blob stores from the backup business
	// logic
	Reference<BackupContainerFileSystem> bstore;
	KeyRangeMap<GranuleRangeMetadata> granuleMetadata;

	// contains the history of completed granules before the existing ones. Maps to the latest one, and has
	// back-pointers to earlier granules
	// FIXME: expire from map after a delay when granule is revoked and the history is no longer needed
	KeyRangeMap<Reference<GranuleHistoryEntry>> granuleHistory;

	PromiseStream<AssignBlobRangeRequest> granuleUpdateErrors;

	Promise<Void> doGRVCheck;
	NotifiedVersion grvVersion;
	Promise<Void> fatalError;

	FlowLock initialSnapshotLock;

	int changeFeedStreamReplyBufferSize = SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES / 2;

	BlobWorkerData(UID id, Database db)
	  : id(id), db(db), stats(id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL),
	    initialSnapshotLock(SERVER_KNOBS->BLOB_WORKER_INITIAL_SNAPSHOT_PARALLELISM) {}

	bool managerEpochOk(int64_t epoch) {
		if (epoch < currentManagerEpoch) {
			if (BW_DEBUG) {
				fmt::print("BW {0} got request from old epoch {1}, notifying them they are out of date\n",
				           id.toString(),
				           epoch);
			}
			return false;
		} else {
			if (epoch > currentManagerEpoch) {
				currentManagerEpoch = epoch;
				if (BW_DEBUG) {
					fmt::print("BW {0} found new manager epoch {1}\n", id.toString(), currentManagerEpoch);
				}
				TraceEvent(SevDebug, "BlobWorkerFoundNewManager", id).detail("Epoch", epoch);
			}

			return true;
		}
	}
};

// serialize change feed key as UID bytes, to use 16 bytes on disk
static Key granuleIDToCFKey(UID granuleID) {
	BinaryWriter wr(Unversioned());
	wr << granuleID;
	return wr.toValue();
}

// parse change feed key back to UID, to be human-readable
static UID cfKeyToGranuleID(Key cfKey) {
	return BinaryReader::fromStringRef<UID>(cfKey, Unversioned());
}

// returns true if we can acquire it
static void acquireGranuleLock(int64_t epoch, int64_t seqno, int64_t prevOwnerEpoch, int64_t prevOwnerSeqno) {
	// returns true if our lock (E, S) >= (Eprev, Sprev)
	if (epoch < prevOwnerEpoch || (epoch == prevOwnerEpoch && seqno < prevOwnerSeqno)) {
		if (BW_DEBUG) {
			fmt::print("Lock acquire check failed. Proposed ({0}, {1}) < previous ({2}, {3})\n",
			           epoch,
			           seqno,
			           prevOwnerEpoch,
			           prevOwnerSeqno);
		}
		throw granule_assignment_conflict();
	}
}

static void checkGranuleLock(int64_t epoch, int64_t seqno, int64_t ownerEpoch, int64_t ownerSeqno) {
	// sanity check - lock value should never go backwards because of acquireGranuleLock
	ASSERT(epoch <= ownerEpoch);
	ASSERT(epoch < ownerEpoch || (epoch == ownerEpoch && seqno <= ownerSeqno));

	// returns true if we still own the lock, false if someone else does
	if (epoch != ownerEpoch || seqno != ownerSeqno) {
		if (BW_DEBUG) {
			fmt::print("Lock assignment check failed. Expected ({0}, {1}), got ({2}, {3})\n",
			           epoch,
			           seqno,
			           ownerEpoch,
			           ownerSeqno);
		}
		throw granule_assignment_conflict();
	}
}

ACTOR Future<Void> readAndCheckGranuleLock(Reference<ReadYourWritesTransaction> tr,
                                           KeyRange granuleRange,
                                           int64_t epoch,
                                           int64_t seqno) {
	state Key lockKey = blobGranuleLockKeyFor(granuleRange);
	Optional<Value> lockValue = wait(tr->get(lockKey));

	ASSERT(lockValue.present());
	std::tuple<int64_t, int64_t, UID> currentOwner = decodeBlobGranuleLockValue(lockValue.get());
	checkGranuleLock(epoch, seqno, std::get<0>(currentOwner), std::get<1>(currentOwner));

	// if we still own the lock, add a conflict range in case anybody else takes it over while we add this file
	// FIXME: we don't need these conflict ranges
	tr->addReadConflictRange(singleKeyRange(lockKey));

	return Void();
}

// Read snapshot and delta files for granule history, for completed granule
// Retries on error local to this function
ACTOR Future<GranuleFiles> loadHistoryFiles(Reference<BlobWorkerData> bwData, UID granuleID) {
	state Transaction tr(bwData->db);
	state KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
	state Key startKey = range.begin;
	state GranuleFiles files;
	loop {
		try {
			wait(readGranuleFiles(&tr, &startKey, range.end, &files, granuleID));
			return files;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// read snapshot and delta files from previous owner of the active granule
// This is separated out from above because this is done as part of granule open transaction
ACTOR Future<GranuleFiles> loadPreviousFiles(Transaction* tr, UID granuleID) {
	state KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
	// no need to add conflict range for read b/c of granule lock
	state Key startKey = range.begin;
	state GranuleFiles files;
	wait(readGranuleFiles(tr, &startKey, range.end, &files, granuleID));
	return files;
}

// To cleanup of the old change feed for the old granule range, all new sub-granules split from the old range must
// update shared state to coordinate when it is safe to clean up the old change feed.
//  his goes through 3 phases for each new sub-granule:
//  1. Starting - the blob manager writes all sub-granules with this state as a durable intent to split the range
//  2. Assigned - a worker that is assigned a sub-granule updates that granule's state here. This means that the
//  worker
//     has started a new change feed for the new sub-granule, but still needs to consume from the old change feed.
//  3. Done - the worker that is assigned this sub-granule has persisted all of the data from its part of the old
//  change
//     feed in delta files. From this granule's perspective, it is safe to clean up the old change feed.

// Once all sub-granules have reached step 2 (Assigned), the change feed can be safely "stopped" - it needs to
// continue to serve the mutations it has seen so far, but will not need any new mutations after this version. The
// last sub-granule to reach this step is responsible for commiting the change feed stop as part of its transaction.
// Because this change feed stops commits in the same transaction as the worker's new change feed start, it is
// guaranteed that no versions are missed between the old and new change feed.
//
// Once all sub-granules have reached step 3 (Done), the change feed can be safely destroyed, as all of the
// mutations in the old change feed are guaranteed to be persisted in delta files. The last sub-granule to reach
// this step is responsible for committing the change feed destroy, and for cleaning up the split state for all
// sub-granules as part of its transaction.

ACTOR Future<Void> updateGranuleSplitState(Transaction* tr,
                                           KeyRange parentGranuleRange,
                                           UID parentGranuleID,
                                           UID currentGranuleID,
                                           BlobGranuleSplitState newState) {
	state KeyRange currentRange = blobGranuleSplitKeyRangeFor(parentGranuleID);

	state RangeResult totalState = wait(tr->getRange(currentRange, SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 1));
	// FIXME: remove above conflict range?
	tr->addWriteConflictRange(currentRange);
	ASSERT_WE_THINK(!totalState.more && totalState.size() <= SERVER_KNOBS->BG_MAX_SPLIT_FANOUT);
	// maybe someone decreased the knob, we should gracefully handle it not in simulation
	if (totalState.more || totalState.size() > SERVER_KNOBS->BG_MAX_SPLIT_FANOUT) {
		RangeResult tryAgain = wait(tr->getRange(currentRange, 10000));
		ASSERT(!tryAgain.more);
		totalState = tryAgain;
	}

	if (totalState.empty()) {
		ASSERT(newState == BlobGranuleSplitState::Done);
		if (BW_DEBUG) {
			fmt::print("Found empty split state for parent granule {0}\n", parentGranuleID.toString());
		}
		// must have retried and successfully nuked everything
		return Void();
	}
	ASSERT(totalState.size() >= 2);

	int total = totalState.size();
	int totalStarted = 0;
	int totalDone = 0;
	BlobGranuleSplitState currentState = BlobGranuleSplitState::Unknown;
	for (auto& it : totalState) {
		UID pid;
		UID cid;
		std::pair<UID, UID> k = decodeBlobGranuleSplitKey(it.key);
		pid = k.first;
		cid = k.second;
		ASSERT(pid == parentGranuleID);

		BlobGranuleSplitState st = decodeBlobGranuleSplitValue(it.value).first;
		ASSERT(st != BlobGranuleSplitState::Unknown);
		if (st == BlobGranuleSplitState::Initialized) {
			totalStarted++;
		} else if (st == BlobGranuleSplitState::Done) {
			totalDone++;
		}
		if (cid == currentGranuleID) {
			ASSERT(currentState == BlobGranuleSplitState::Unknown);
			currentState = st;
		}
	}

	ASSERT(currentState != BlobGranuleSplitState::Unknown);

	if (currentState < newState) {
		if (BW_DEBUG) {
			fmt::print("Updating granule {0} split state from {1} {2} -> {3}\n",
			           currentGranuleID.toString(),
			           parentGranuleID.toString(),
			           currentState,
			           newState);
		}

		Key myStateKey = blobGranuleSplitKeyFor(parentGranuleID, currentGranuleID);
		if (newState == BlobGranuleSplitState::Done && currentState == BlobGranuleSplitState::Assigned &&
		    totalDone == total - 1) {
			// we are the last one to change from Assigned -> Done, so everything can be cleaned up for the old
			// change feed and splitting state
			if (BW_DEBUG) {
				fmt::print("{0} destroying old granule {1}\n", currentGranuleID.toString(), parentGranuleID.toString());
			}

			// FIXME: appears change feed destroy isn't working! ADD BACK
			// wait(updateChangeFeed(tr, granuleIDToCFKey(parentGranuleID), ChangeFeedStatus::CHANGE_FEED_DESTROY));

			Key oldGranuleLockKey = blobGranuleLockKeyFor(parentGranuleRange);
			// FIXME: deleting granule lock can cause races where another granule with the same range starts way later
			// and thinks it can own the granule! Need to change file cleanup to destroy these, if there is no more
			// granule in the history with that exact key range!
			// Alternative fix could be to, on granule open, query for all overlapping granule locks and ensure none of
			// them have higher (epoch, seqno), but that is much more expensive

			// tr->clear(singleKeyRange(oldGranuleLockKey));
			tr->clear(currentRange);
			TEST(true); // Granule split cleanup on last delta file persisted
		} else {
			tr->atomicOp(myStateKey, blobGranuleSplitValueFor(newState), MutationRef::SetVersionstampedValue);
			if (newState == BlobGranuleSplitState::Assigned && currentState == BlobGranuleSplitState::Initialized &&
			    totalStarted == 1) {
				// We are the last one to change from Start -> Assigned, so we can stop the parent change feed.
				if (BW_DEBUG) {
					fmt::print("{0} stopping change feed for old granule {1}\n",
					           currentGranuleID.toString().c_str(),
					           parentGranuleID.toString().c_str());
				}

				wait(updateChangeFeed(
				    tr, KeyRef(granuleIDToCFKey(parentGranuleID)), ChangeFeedStatus::CHANGE_FEED_STOP));
			}
			TEST(true); // Granule split stopping change feed
		}
	} else if (BW_DEBUG) {
		TEST(true); // Out of order granule split state updates ignored
		fmt::print("Ignoring granule {0} split state from {1} {2} -> {3}\n",
		           currentGranuleID.toString(),
		           parentGranuleID.toString(),
		           currentState,
		           newState);
	}

	return Void();
}

// Returns the split state for a given granule on granule reassignment, or unknown if it doesn't exist (meaning the
// granule splitting finished)
ACTOR Future<std::pair<BlobGranuleSplitState, Version>> getGranuleSplitState(Transaction* tr,
                                                                             UID parentGranuleID,
                                                                             UID currentGranuleID) {
	Key myStateKey = blobGranuleSplitKeyFor(parentGranuleID, currentGranuleID);

	Optional<Value> st = wait(tr->get(myStateKey));
	if (st.present()) {
		return decodeBlobGranuleSplitValue(st.get());
	} else {
		return std::pair(BlobGranuleSplitState::Unknown, invalidVersion);
	}
}

// writeDelta file writes speculatively in the common case to optimize throughput. It creates the s3 object even though
// the data in it may not yet be committed, and even though previous delta files with lower versioned data may still be
// in flight. The synchronization happens after the s3 file is written, but before we update the FDB index of what files
// exist. Before updating FDB, we ensure the version is committed and all previous delta files have updated FDB.
ACTOR Future<BlobFileIndex> writeDeltaFile(Reference<BlobWorkerData> bwData,
                                           KeyRange keyRange,
                                           UID granuleID,
                                           int64_t epoch,
                                           int64_t seqno,
                                           Standalone<GranuleDeltas> deltasToWrite,
                                           Version currentDeltaVersion,
                                           Future<BlobFileIndex> previousDeltaFileFuture,
                                           Future<Void> waitCommitted,
                                           Optional<std::pair<KeyRange, UID>> oldGranuleComplete) {
	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

	// Prefix filename with random chars both to avoid hotspotting on granuleID, and to have unique file names if
	// multiple blob workers try to create the exact same file at the same millisecond (which observably happens)
	state std::string fname = deterministicRandom()->randomUniqueID().shortString() + "_" + granuleID.toString() +
	                          "_T" + std::to_string((uint64_t)(1000.0 * now())) + "_V" +
	                          std::to_string(currentDeltaVersion) + ".delta";

	state Value serialized = ObjectWriter::toValue(deltasToWrite, Unversioned());
	state size_t serializedSize = serialized.size();

	// Free up deltasToWrite here to reduce memory
	deltasToWrite = Standalone<GranuleDeltas>();

	state Reference<IBackupFile> objectFile = wait(bwData->bstore->writeFile(fname));

	++bwData->stats.s3PutReqs;
	++bwData->stats.deltaFilesWritten;
	bwData->stats.deltaBytesWritten += serializedSize;

	wait(objectFile->append(serialized.begin(), serializedSize));
	wait(objectFile->finish());

	// free serialized since it is persisted in blob
	serialized = Value();

	state int numIterations = 0;
	try {
		// before updating FDB, wait for the delta file version to be committed and previous delta files to finish
		wait(waitCommitted);
		BlobFileIndex prev = wait(previousDeltaFileFuture);
		wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));

		// update FDB with new file
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));
				numIterations++;

				Key dfKey = blobGranuleFileKeyFor(granuleID, currentDeltaVersion, 'D');
				// TODO change once we support file multiplexing
				Value dfValue = blobGranuleFileValueFor(fname, 0, serializedSize, serializedSize);
				tr->set(dfKey, dfValue);

				if (oldGranuleComplete.present()) {
					wait(updateGranuleSplitState(&tr->getTransaction(),
					                             oldGranuleComplete.get().first,
					                             oldGranuleComplete.get().second,
					                             granuleID,
					                             BlobGranuleSplitState::Done));
				}

				wait(tr->commit());
				if (BW_DEBUG) {
					fmt::print(
					    "Granule {0} [{1} - {2}) updated fdb with delta file {3} of size {4} at version {5}, cv={6}\n",
					    granuleID.toString(),
					    keyRange.begin.printable(),
					    keyRange.end.printable(),
					    fname,
					    serializedSize,
					    currentDeltaVersion,
					    tr->getCommittedVersion());
				}

				if (BUGGIFY_WITH_PROB(0.01)) {
					wait(delay(deterministicRandom()->random01()));
				}
				// FIXME: change when we implement multiplexing
				return BlobFileIndex(currentDeltaVersion, fname, 0, serializedSize, serializedSize);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// If this actor was cancelled, doesn't own the granule anymore, or got some other error before trying to
		// commit a transaction, we can and want to safely delete the file we wrote. Otherwise, we may have updated FDB
		// with file and cannot safely delete it.
		if (numIterations > 0) {
			TEST(true); // Granule potentially leaving orphaned delta file
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("deleting delta file {0} after error {1}\n", fname, e.name());
		}
		TEST(true); // Granule cleaning up delta file after error
		++bwData->stats.s3DeleteReqs;
		bwData->addActor.send(bwData->bstore->deleteFile(fname));
		throw e;
	}
}

ACTOR Future<BlobFileIndex> writeSnapshot(Reference<BlobWorkerData> bwData,
                                          KeyRange keyRange,
                                          UID granuleID,
                                          int64_t epoch,
                                          int64_t seqno,
                                          Version version,
                                          PromiseStream<RangeResult> rows,
                                          bool createGranuleHistory) {
	// Prefix filename with random chars both to avoid hotspotting on granuleID, and to have unique file names if
	// multiple blob workers try to create the exact same file at the same millisecond (which observably happens)
	state std::string fname = deterministicRandom()->randomUniqueID().shortString() + "_" + granuleID.toString() +
	                          "_T" + std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(version) +
	                          ".snapshot";
	state Standalone<GranuleSnapshot> snapshot;

	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

	loop {
		try {
			RangeResult res = waitNext(rows.getFuture());
			snapshot.arena().dependsOn(res.arena());
			snapshot.append(snapshot.arena(), res.begin(), res.size());
			wait(yield(TaskPriority::BlobWorkerUpdateStorage));
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			throw e;
		}
	}

	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) read {2} snapshot rows\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           snapshot.size());
	}

	if (g_network->isSimulated()) {
		if (snapshot.size() > 0) {
			ASSERT(keyRange.begin <= snapshot[0].key);
			ASSERT(keyRange.end > snapshot[snapshot.size() - 1].key);
		}
		for (int i = 0; i < snapshot.size() - 1; i++) {
			if (snapshot[i].key >= snapshot[i + 1].key) {
				fmt::print("SORT ORDER VIOLATION IN SNAPSHOT FILE: {0}, {1}\n",
				           snapshot[i].key.printable(),
				           snapshot[i + 1].key.printable());
			}
			ASSERT(snapshot[i].key < snapshot[i + 1].key);
		}
	}

	state Value serialized = ObjectWriter::toValue(snapshot, Unversioned());
	state size_t serializedSize = serialized.size();

	// free snapshot to reduce memory
	snapshot = Standalone<GranuleSnapshot>();

	// write to blob using multi part upload
	state Reference<IBackupFile> objectFile = wait(bwData->bstore->writeFile(fname));

	++bwData->stats.s3PutReqs;
	++bwData->stats.snapshotFilesWritten;
	bwData->stats.snapshotBytesWritten += serializedSize;

	wait(objectFile->append(serialized.begin(), serializedSize));
	wait(objectFile->finish());

	// free serialized since it is persisted in blob
	serialized = Value();

	wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));
	// object uploaded successfully, save it to system key space

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	state int numIterations = 0;

	try {
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));
				numIterations++;
				Key snapshotFileKey = blobGranuleFileKeyFor(granuleID, version, 'S');
				// TODO change once we support file multiplexing
				Key snapshotFileValue = blobGranuleFileValueFor(fname, 0, serializedSize, serializedSize);
				tr->set(snapshotFileKey, snapshotFileValue);
				// create granule history at version if this is a new granule with the initial dump from FDB
				if (createGranuleHistory) {
					Key historyKey = blobGranuleHistoryKeyFor(keyRange, version);
					Standalone<BlobGranuleHistoryValue> historyValue;
					historyValue.granuleID = granuleID;
					tr->set(historyKey, blobGranuleHistoryValueFor(historyValue));
				}
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// If this actor was cancelled, doesn't own the granule anymore, or got some other error before trying to
		// commit a transaction, we can and want to safely delete the file we wrote. Otherwise, we may have updated FDB
		// with file and cannot safely delete it.
		if (numIterations > 0) {
			TEST(true); // Granule potentially leaving orphaned snapshot file
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("deleting snapshot file {0} after error {1}\n", fname, e.name());
		}
		TEST(true); // Granule deleting snapshot file after error
		++bwData->stats.s3DeleteReqs;
		bwData->addActor.send(bwData->bstore->deleteFile(fname));
		throw e;
	}

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) committed new snapshot file {2} with {3} bytes\n\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           fname,
		           serializedSize);
	}

	if (BUGGIFY_WITH_PROB(0.1)) {
		wait(delay(deterministicRandom()->random01()));
	}

	// FIXME: change when we implement multiplexing
	return BlobFileIndex(version, fname, 0, serializedSize, serializedSize);
}

ACTOR Future<BlobFileIndex> dumpInitialSnapshotFromFDB(Reference<BlobWorkerData> bwData,
                                                       Reference<GranuleMetadata> metadata,
                                                       UID granuleID,
                                                       Key cfKey) {
	if (BW_DEBUG) {
		fmt::print("Dumping snapshot from FDB for [{0} - {1})\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable());
	}
	wait(bwData->initialSnapshotLock.take());
	state FlowLock::Releaser holdingDVL(bwData->initialSnapshotLock);

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	state int64_t bytesRead = 0;
	state int retries = 0;
	state Version lastReadVersion = invalidVersion;
	state Version readVersion = invalidVersion;

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			Version rv = wait(tr->getReadVersion());
			readVersion = rv;
			ASSERT(lastReadVersion <= readVersion);
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(bwData,
			                                                           metadata->keyRange,
			                                                           granuleID,
			                                                           metadata->originalEpoch,
			                                                           metadata->originalSeqno,
			                                                           readVersion,
			                                                           rowsStream,
			                                                           true);
			Future<Void> streamFuture =
			    tr->getTransaction().getRangeStream(rowsStream, metadata->keyRange, GetRangeLimits(), Snapshot::True);
			wait(streamFuture && success(snapshotWriter));
			TraceEvent(SevDebug, "BlobGranuleSnapshotFile", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("Version", readVersion);
			DEBUG_KEY_RANGE("BlobWorkerFDBSnapshot", readVersion, metadata->keyRange, bwData->id);

			// initial snapshot is committed in fdb, we can pop the change feed up to this version
			bwData->addActor.send(bwData->db->popChangeFeedMutations(cfKey, readVersion));
			return snapshotWriter.get();
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			if (BW_DEBUG) {
				fmt::print("Dumping snapshot {0} from FDB for [{1} - {2}) got error {3} after {4} bytes\n",
				           retries + 1,
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable(),
				           e.name(),
				           bytesRead);
			}
			state Error err = e;
			wait(tr->onError(e));
			retries++;
			TEST(true); // Granule initial snapshot failed
			// FIXME: why can't we supress error event?
			TraceEvent(retries < 10 ? SevDebug : SevWarn, "BlobGranuleInitialSnapshotRetry", bwData->id)
			    .error(err)
			    .detail("Granule", metadata->keyRange)
			    .detail("Count", retries);
			bytesRead = 0;
			lastReadVersion = readVersion;
			// Pop change feed up to readVersion, because that data will be before the next snapshot
			// Do this to prevent a large amount of CF data from accumulating if we have consecutive failures to
			// snapshot
			// Also somewhat servers as a rate limiting function and checking that the database is available for this
			// key range
			wait(bwData->db->popChangeFeedMutations(cfKey, readVersion));
		}
	}
}

// files might not be the current set of files in metadata, in the case of doing the initial snapshot of a granule that
// was split.
ACTOR Future<BlobFileIndex> compactFromBlob(Reference<BlobWorkerData> bwData,
                                            Reference<GranuleMetadata> metadata,
                                            UID granuleID,
                                            GranuleFiles files,
                                            Version version) {
	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));
	if (BW_DEBUG) {
		fmt::print("Compacting snapshot from blob for [{0} - {1})\n",
		           metadata->keyRange.begin.printable().c_str(),
		           metadata->keyRange.end.printable().c_str());
	}

	ASSERT(!files.snapshotFiles.empty());
	ASSERT(!files.deltaFiles.empty());

	state Arena filenameArena;
	state BlobGranuleChunkRef chunk;

	state int64_t compactBytesRead = 0;
	state Version snapshotVersion = files.snapshotFiles.back().version;
	BlobFileIndex snapshotF = files.snapshotFiles.back();

	ASSERT(snapshotVersion < version);

	chunk.snapshotFile = BlobFilePointerRef(
	    filenameArena, snapshotF.filename, snapshotF.offset, snapshotF.length, snapshotF.fullFileLength);
	compactBytesRead += snapshotF.length;
	int deltaIdx = files.deltaFiles.size() - 1;
	while (deltaIdx >= 0 && files.deltaFiles[deltaIdx].version > snapshotVersion) {
		deltaIdx--;
	}
	deltaIdx++;
	Version lastDeltaVersion = invalidVersion;
	while (deltaIdx < files.deltaFiles.size() && files.deltaFiles[deltaIdx].version <= version) {
		BlobFileIndex deltaF = files.deltaFiles[deltaIdx];
		chunk.deltaFiles.emplace_back_deep(
		    filenameArena, deltaF.filename, deltaF.offset, deltaF.length, deltaF.fullFileLength);
		compactBytesRead += deltaF.length;
		lastDeltaVersion = files.deltaFiles[deltaIdx].version;
		deltaIdx++;
	}
	ASSERT(lastDeltaVersion == version);
	chunk.includedVersion = version;

	if (BW_DEBUG) {
		fmt::print("Re-snapshotting [{0} - {1}) @ {2} from blob\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           version);
	}

	loop {
		try {
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(bwData,
			                                                           metadata->keyRange,
			                                                           granuleID,
			                                                           metadata->originalEpoch,
			                                                           metadata->originalSeqno,
			                                                           version,
			                                                           rowsStream,
			                                                           false);
			RangeResult newGranule =
			    wait(readBlobGranule(chunk, metadata->keyRange, 0, version, bwData->bstore, &bwData->stats));

			bwData->stats.bytesReadFromS3ForCompaction += compactBytesRead;
			rowsStream.send(std::move(newGranule));
			rowsStream.sendError(end_of_stream());

			BlobFileIndex f = wait(snapshotWriter);
			DEBUG_KEY_RANGE("BlobWorkerBlobSnapshot", version, metadata->keyRange, bwData->id);
			return f;
		} catch (Error& e) {
			if (BW_DEBUG) {
				fmt::print("Compacting snapshot from blob for [{0} - {1}) got error {2}\n",
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable(),
				           e.name());
			}
			throw e;
		}
	}
}

ACTOR Future<BlobFileIndex> checkSplitAndReSnapshot(Reference<BlobWorkerData> bwData,
                                                    Reference<GranuleMetadata> metadata,
                                                    UID granuleID,
                                                    int64_t bytesInNewDeltaFiles,
                                                    Future<BlobFileIndex> lastDeltaBeforeSnapshot,
                                                    int64_t versionsSinceLastSnapshot) {

	BlobFileIndex lastDeltaIdx = wait(lastDeltaBeforeSnapshot);
	state Version reSnapshotVersion = lastDeltaIdx.version;
	while (!bwData->statusStreamInitialized) {
		wait(bwData->currentManagerStatusStream.onChange());
	}

	wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) checking with BM for re-snapshot after {2} bytes\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           metadata->bytesInNewDeltaFiles);
	}

	TraceEvent(SevDebug, "BlobGranuleSnapshotCheck", bwData->id)
	    .detail("Granule", metadata->keyRange)
	    .detail("Version", reSnapshotVersion);

	// Save these from the start so repeated requests are idempotent
	// Need to retry in case response is dropped or manager changes. Eventually, a manager will
	// either reassign the range with continue=true, or will revoke the range. But, we will keep the
	// range open at this version for reads until that assignment change happens
	metadata->resumeSnapshot.reset();
	state int64_t statusEpoch = metadata->continueEpoch;
	state int64_t statusSeqno = metadata->continueSeqno;

	// If two snapshots happen without a split within a low time interval, this granule is "write-hot"
	// FIXME: If a rollback happens, this could incorrectly identify a hot granule as not hot. This should be rare
	// though and is just less efficient.
	state bool writeHot = versionsSinceLastSnapshot <= SERVER_KNOBS->BG_HOT_SNAPSHOT_VERSIONS;
	// FIXME: could probably refactor all of this logic into one large choose/when state machine that's less complex
	loop {
		loop {
			try {
				// wait for manager stream to become ready, and send a message
				loop {
					choose {
						when(wait(bwData->currentManagerStatusStream.get().onReady())) { break; }
						when(wait(bwData->currentManagerStatusStream.onChange())) {}
						when(wait(metadata->resumeSnapshot.getFuture())) { break; }
					}
				}
				if (metadata->resumeSnapshot.isSet()) {
					break;
				}

				bwData->currentManagerStatusStream.get().send(GranuleStatusReply(metadata->keyRange,
				                                                                 true,
				                                                                 writeHot,
				                                                                 statusEpoch,
				                                                                 statusSeqno,
				                                                                 granuleID,
				                                                                 metadata->initialSnapshotVersion));
				break;
			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}
				TEST(true); // Blob worker re-sending split evaluation to manager after not error/not hearing back
				// if we got broken promise while waiting, the old stream was killed, so we don't need to wait on
				// change, just retry
				if (e.code() == error_code_broken_promise) {
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				} else {
					wait(bwData->currentManagerStatusStream.onChange());
				}
			}
		}

		// wait for manager reply (which will either cancel this future or call resumeSnapshot), or re-send on manager
		// change/no response
		choose {
			when(wait(bwData->currentManagerStatusStream.onChange())) {}
			when(wait(metadata->resumeSnapshot.getFuture())) { break; }
			when(wait(delay(1.0))) {}
		}

		if (BW_DEBUG) {
			fmt::print("Granule [{0} - {1})\n, hasn't heard back from BM in BW {2}, re-sending status\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           bwData->id.toString());
		}
	}

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) re-snapshotting after {2} bytes\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           bytesInNewDeltaFiles);
	}
	TraceEvent(SevDebug, "BlobGranuleSnapshotFile", bwData->id)
	    .detail("Granule", metadata->keyRange)
	    .detail("Version", metadata->durableDeltaVersion.get());

	// wait for file updater to make sure that last delta file is in the metadata before
	while (metadata->files.deltaFiles.empty() || metadata->files.deltaFiles.back().version < reSnapshotVersion) {
		wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
	}
	BlobFileIndex reSnapshotIdx =
	    wait(compactFromBlob(bwData, metadata, granuleID, metadata->files, reSnapshotVersion));
	return reSnapshotIdx;
}

static void handleCompletedDeltaFile(Reference<BlobWorkerData> bwData,
                                     Reference<GranuleMetadata> metadata,
                                     BlobFileIndex completedDeltaFile,
                                     Key cfKey,
                                     Version cfStartVersion,
                                     std::deque<std::pair<Version, Version>>* rollbacksCompleted) {
	metadata->files.deltaFiles.push_back(completedDeltaFile);
	ASSERT(metadata->durableDeltaVersion.get() < completedDeltaFile.version);
	metadata->durableDeltaVersion.set(completedDeltaFile.version);

	if (completedDeltaFile.version > cfStartVersion) {
		if (BW_DEBUG) {
			fmt::print("Popping change feed {0} at {1}\n",
			           cfKeyToGranuleID(cfKey).toString().c_str(),
			           completedDeltaFile.version);
		}
		// FIXME: for a write-hot shard, we could potentially batch these and only pop the largest one after several
		// have completed
		// FIXME: we actually want to pop at this version + 1 because pop is exclusive?
		// FIXME: since this is async, and worker could die, new blob worker that opens granule should probably kick off
		// an async pop at its previousDurableVersion after opening the granule to guarantee it is eventually popped?
		Future<Void> popFuture = bwData->db->popChangeFeedMutations(cfKey, completedDeltaFile.version);
		// Do pop asynchronously
		bwData->addActor.send(popFuture);
	}
	while (!rollbacksCompleted->empty() && completedDeltaFile.version >= rollbacksCompleted->front().second) {
		if (BW_DEBUG) {
			fmt::print("Granule [{0} - {1}) on BW {2} completed rollback {3} -> {4} with delta file {5}\n",
			           metadata->keyRange.begin.printable().c_str(),
			           metadata->keyRange.end.printable().c_str(),
			           bwData->id.toString().substr(0, 5).c_str(),
			           rollbacksCompleted->front().second,
			           rollbacksCompleted->front().first,
			           completedDeltaFile.version);
		}
		rollbacksCompleted->pop_front();
	}
}

// if we get an i/o error updating files, or a rollback, reassign the granule to ourselves and start fresh
static bool granuleCanRetry(const Error& e) {
	switch (e.code()) {
	case error_code_please_reboot:
	case error_code_io_error:
	case error_code_io_timeout:
	case error_code_http_request_failed:
		return true;
	default:
		return false;
	};
}

struct InFlightFile {
	Future<BlobFileIndex> future;
	Version version;
	uint64_t bytes;
	bool snapshot;

	InFlightFile(Future<BlobFileIndex> future, Version version, uint64_t bytes, bool snapshot)
	  : future(future), version(version), bytes(bytes), snapshot(snapshot) {}
};

static Version doGranuleRollback(Reference<GranuleMetadata> metadata,
                                 Version mutationVersion,
                                 Version rollbackVersion,
                                 std::deque<InFlightFile>& inFlightFiles,
                                 std::deque<std::pair<Version, Version>>& rollbacksInProgress,
                                 std::deque<std::pair<Version, Version>>& rollbacksCompleted) {
	Version cfRollbackVersion;
	if (metadata->pendingDeltaVersion > rollbackVersion) {
		// if we already started writing mutations to a delta or snapshot file with version > rollbackVersion,
		// we need to rescind those delta file writes
		ASSERT(!inFlightFiles.empty());
		cfRollbackVersion = metadata->durableDeltaVersion.get();
		metadata->pendingSnapshotVersion = metadata->durableSnapshotVersion.get();
		int toPop = 0;
		bool pendingSnapshot = false;
		for (auto& f : inFlightFiles) {
			if (f.snapshot) {
				if (f.version > rollbackVersion) {
					TEST(true); // Granule rollback cancelling snapshot file
					if (BW_DEBUG) {
						fmt::print("[{0} - {1}) rollback cancelling snapshot file @ {2}\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
						           f.version);
					}
					f.future.cancel();
					toPop++;
				} else {
					metadata->pendingSnapshotVersion = f.version;
					metadata->bytesInNewDeltaFiles = 0;
					pendingSnapshot = true;
				}
			} else {
				if (f.version > rollbackVersion) {
					f.future.cancel();
					if (!pendingSnapshot) {
						metadata->bytesInNewDeltaFiles -= f.bytes;
					}
					toPop++;
					TEST(true); // Granule rollback cancelling delta file
					if (BW_DEBUG) {
						fmt::print("[{0} - {1}) rollback cancelling delta file @ {2}\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
						           f.version);
					}
				} else {
					ASSERT(f.version > cfRollbackVersion);
					cfRollbackVersion = f.version;
					if (pendingSnapshot) {
						metadata->bytesInNewDeltaFiles += f.bytes;
					}
				}
			}
		}
		ASSERT(toPop > 0);
		while (toPop > 0) {
			inFlightFiles.pop_back();
			toPop--;
		}
		metadata->pendingDeltaVersion = cfRollbackVersion;
		if (BW_DEBUG) {
			fmt::print("[{0} - {1}) rollback discarding all {2} in-memory mutations\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           metadata->currentDeltas.size());
		}

		// discard all in-memory mutations
		metadata->currentDeltas = Standalone<GranuleDeltas>();
		metadata->bufferedDeltaBytes = 0;
		metadata->bufferedDeltaVersion = cfRollbackVersion;

		// Track that this rollback happened, since we have to re-read mutations up to the rollback
		// Add this rollback to in progress, and put all completed ones back in progress
		rollbacksInProgress.push_back(std::pair(rollbackVersion, mutationVersion));
		while (!rollbacksCompleted.empty()) {
			if (rollbacksCompleted.back().first >= cfRollbackVersion) {
				rollbacksInProgress.push_front(rollbacksCompleted.back());
				rollbacksCompleted.pop_back();
			} else {
				// some rollbacks in completed could still have a delta file in flight after this rollback, they should
				// remain in completed
				break;
			}
		}

	} else {
		// No pending delta files to discard, just in-memory mutations
		TEST(true); // Granule rollback discarding in memory mutations

		// FIXME: could binary search?
		int mIdx = metadata->currentDeltas.size() - 1;
		while (mIdx >= 0) {
			if (metadata->currentDeltas[mIdx].version <= rollbackVersion) {
				break;
			}
			for (auto& m : metadata->currentDeltas[mIdx].mutations) {
				metadata->bufferedDeltaBytes -= m.totalSize();
			}
			mIdx--;
		}
		mIdx++;
		if (BW_DEBUG) {
			fmt::print("[{0} - {1}) rollback discarding {2} in-memory mutations, {3} mutations and {4} bytes left\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           metadata->currentDeltas.size() - mIdx,
			           mIdx,
			           metadata->bufferedDeltaBytes);
		}

		metadata->currentDeltas.resize(metadata->currentDeltas.arena(), mIdx);

		// delete all deltas in rollback range, but we can optimize here to just skip the uncommitted mutations
		// directly and immediately pop the rollback out of inProgress to completed

		metadata->bufferedDeltaVersion = rollbackVersion;
		cfRollbackVersion = mutationVersion;
		rollbacksCompleted.push_back(std::pair(rollbackVersion, mutationVersion));
	}

	if (BW_DEBUG) {
		fmt::print("[{0} - {1}) finishing rollback to {2}\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           cfRollbackVersion);
	}

	return cfRollbackVersion;
}

ACTOR Future<Void> waitOnCFVersion(Reference<GranuleMetadata> metadata, Version waitVersion) {
	loop {
		try {
			// if not valid, we're about to be cancelled anyway
			state Future<Void> atLeast = metadata->activeCFData.get().isValid()
			                                 ? metadata->activeCFData.get()->whenAtLeast(waitVersion)
			                                 : Never();
			choose {
				when(wait(atLeast)) { break; }
				when(wait(metadata->activeCFData.onChange())) {}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled || e.code() == error_code_change_feed_popped) {
				throw e;
			}

			// if waiting on a parent granule change feed and we change to the child, the parent will get end_of_stream,
			// which could cause this waiting whenAtLeast to get change_feed_cancelled. We should simply retry and wait
			// a bit, as blobGranuleUpdateFiles will switch to the new change feed
			wait(delay(0.05));
		}
	}

	// stop after change feed callback
	wait(delay(0, TaskPriority::BlobWorkerReadChangeFeed));

	return Void();
}

ACTOR Future<Void> waitCommittedGrv(Reference<BlobWorkerData> bwData,
                                    Reference<GranuleMetadata> metadata,
                                    Version version) {
	if (version > bwData->grvVersion.get()) {
		// this order is important, since we need to register a waiter on the notified version before waking the GRV
		// actor
		Future<Void> grvAtLeast = bwData->grvVersion.whenAtLeast(version);
		Promise<Void> doGrvCheck = bwData->doGRVCheck;
		if (doGrvCheck.canBeSet()) {
			doGrvCheck.send(Void());
		}
		wait(grvAtLeast);
	}

	Version grvVersion = bwData->grvVersion.get();
	wait(waitOnCFVersion(metadata, grvVersion));
	return Void();
}

ACTOR Future<Void> waitVersionCommitted(Reference<BlobWorkerData> bwData,
                                        Reference<GranuleMetadata> metadata,
                                        Version version) {
	// If GRV is way in the future, we know we can't roll back more than 5 seconds (or whatever this knob is set to)
	// worth of versions
	wait(waitCommittedGrv(bwData, metadata, version) ||
	     waitOnCFVersion(metadata, version + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS));
	if (version > metadata->knownCommittedVersion) {
		metadata->knownCommittedVersion = version;
	}
	return Void();
}

// updater for a single granule
// TODO: this is getting kind of large. Should try to split out this actor if it continues to grow?
ACTOR Future<Void> blobGranuleUpdateFiles(Reference<BlobWorkerData> bwData,
                                          Reference<GranuleMetadata> metadata,
                                          Future<GranuleStartState> assignFuture) {
	state std::deque<InFlightFile> inFlightFiles;
	state Future<Void> oldChangeFeedFuture;
	state Future<Void> changeFeedFuture;
	state GranuleStartState startState;
	state bool readOldChangeFeed;
	state Optional<std::pair<KeyRange, UID>> oldChangeFeedDataComplete;
	state Key cfKey;
	state Optional<Key> oldCFKey;
	state int pendingSnapshots = 0;

	state std::deque<std::pair<Version, Version>> rollbacksInProgress;
	state std::deque<std::pair<Version, Version>> rollbacksCompleted;

	state bool snapshotEligible; // just wrote a delta file or just took granule over from another worker
	state bool justDidRollback = false;

	try {
		// set resume snapshot so it's not valid until we pause to ask the blob manager for a re-snapshot
		metadata->resumeSnapshot.send(Void());

		// before starting, make sure worker persists range assignment and acquires the granule lock
		GranuleStartState _info = wait(assignFuture);
		startState = _info;

		wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

		cfKey = granuleIDToCFKey(startState.granuleID);
		if (startState.parentGranule.present()) {
			oldCFKey = granuleIDToCFKey(startState.parentGranule.get().second);
		}

		if (BW_DEBUG) {
			fmt::print("Granule File Updater Starting for [{0} - {1}) @ ({2}, {3}):\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           metadata->originalEpoch,
			           metadata->originalSeqno);
			fmt::print("  CFID: {} ({})\n", startState.granuleID.toString(), cfKey.printable());
			fmt::print("  CF Start Version: {}\n", startState.changeFeedStartVersion);
			fmt::print("  Previous Durable Version: {}\n", startState.previousDurableVersion);
			fmt::print("  doSnapshot={}\n", startState.doSnapshot ? "T" : "F");
			fmt::print("  Prev CFID: {}\n",
			           startState.parentGranule.present() ? startState.parentGranule.get().second.toString().c_str()
			                                              : "");
			fmt::print("  blobFilesToSnapshot={}\n", startState.blobFilesToSnapshot.present() ? "T" : "F");
		}

		state Version startVersion;
		state BlobFileIndex newSnapshotFile;

		// if this is a reassign, calculate how close to a snapshot the previous owner was
		if (startState.existingFiles.present()) {
			GranuleFiles files = startState.existingFiles.get();
			if (!files.snapshotFiles.empty() && !files.deltaFiles.empty()) {
				Version snapshotVersion = files.snapshotFiles.back().version;
				for (int i = files.deltaFiles.size() - 1; i >= 0; i--) {
					if (files.deltaFiles[i].version > snapshotVersion) {
						metadata->bytesInNewDeltaFiles += files.deltaFiles[i].length;
					}
				}
			}

			metadata->files = startState.existingFiles.get();
			snapshotEligible = true;
		}

		if (!startState.doSnapshot) {
			TEST(true); // Granule moved without split
			startVersion = startState.previousDurableVersion;
			ASSERT(!metadata->files.snapshotFiles.empty());
			metadata->pendingSnapshotVersion = metadata->files.snapshotFiles.back().version;
			metadata->durableSnapshotVersion.set(metadata->pendingSnapshotVersion);
			metadata->initialSnapshotVersion = metadata->files.snapshotFiles.front().version;
		} else {
			if (startState.blobFilesToSnapshot.present()) {
				startVersion = startState.previousDurableVersion;
				Future<BlobFileIndex> inFlightBlobSnapshot = compactFromBlob(
				    bwData, metadata, startState.granuleID, startState.blobFilesToSnapshot.get(), startVersion);
				inFlightFiles.push_back(InFlightFile(inFlightBlobSnapshot, startVersion, 0, true));
				pendingSnapshots++;

				metadata->durableSnapshotVersion.set(startState.blobFilesToSnapshot.get().snapshotFiles.back().version);
			} else {
				ASSERT(startState.previousDurableVersion == invalidVersion);
				BlobFileIndex fromFDB = wait(dumpInitialSnapshotFromFDB(bwData, metadata, startState.granuleID, cfKey));
				newSnapshotFile = fromFDB;
				ASSERT(startState.changeFeedStartVersion <= fromFDB.version);
				startVersion = newSnapshotFile.version;
				metadata->files.snapshotFiles.push_back(newSnapshotFile);
				metadata->durableSnapshotVersion.set(startVersion);

				wait(yield(TaskPriority::BlobWorkerUpdateStorage));
			}
			metadata->initialSnapshotVersion = startVersion;
			metadata->pendingSnapshotVersion = startVersion;
		}

		metadata->durableDeltaVersion.set(startVersion);
		metadata->pendingDeltaVersion = startVersion;
		metadata->bufferedDeltaVersion = startVersion;
		metadata->knownCommittedVersion = startVersion;

		Reference<ChangeFeedData> cfData = makeReference<ChangeFeedData>();

		if (startState.parentGranule.present() && startVersion < startState.changeFeedStartVersion) {
			// read from parent change feed up until our new change feed is started
			// Required to have canReadPopped = false, otherwise another granule can take over the change feed, and pop
			// it. That could cause this worker to think it has the full correct set of data if it then reads the data,
			// until it checks the granule lock again.
			// passing false for canReadPopped means we will get an exception if we try to read any popped data, killing
			// this actor
			readOldChangeFeed = true;

			oldChangeFeedFuture = bwData->db->getChangeFeedStream(cfData,
			                                                      oldCFKey.get(),
			                                                      startVersion + 1,
			                                                      startState.changeFeedStartVersion,
			                                                      metadata->keyRange,
			                                                      bwData->changeFeedStreamReplyBufferSize,
			                                                      false);

		} else {
			readOldChangeFeed = false;
			changeFeedFuture = bwData->db->getChangeFeedStream(cfData,
			                                                   cfKey,
			                                                   startVersion + 1,
			                                                   MAX_VERSION,
			                                                   metadata->keyRange,
			                                                   bwData->changeFeedStreamReplyBufferSize,
			                                                   false);
		}

		// Start actors BEFORE setting new change feed data to ensure the change feed data is properly initialized by
		// the client
		metadata->activeCFData.set(cfData);

		ASSERT(metadata->readable.canBeSet());
		metadata->readable.send(Void());

		loop {
			// check outstanding snapshot/delta files for completion
			while (inFlightFiles.size() > 0) {
				if (inFlightFiles.front().future.isReady()) {
					BlobFileIndex completedFile = wait(inFlightFiles.front().future);
					if (inFlightFiles.front().snapshot) {
						if (metadata->files.deltaFiles.empty()) {
							ASSERT(completedFile.version == metadata->initialSnapshotVersion);
						} else {
							ASSERT(completedFile.version == metadata->files.deltaFiles.back().version);
						}

						metadata->files.snapshotFiles.push_back(completedFile);
						metadata->durableSnapshotVersion.set(completedFile.version);
						pendingSnapshots--;
					} else {
						handleCompletedDeltaFile(bwData,
						                         metadata,
						                         completedFile,
						                         cfKey,
						                         startState.changeFeedStartVersion,
						                         &rollbacksCompleted);
					}

					inFlightFiles.pop_front();
					wait(yield(TaskPriority::BlobWorkerUpdateStorage));
				} else {
					break;
				}
			}

			// inject delay into reading change feed stream
			if (BUGGIFY_WITH_PROB(0.001)) {
				wait(delay(deterministicRandom()->random01(), TaskPriority::BlobWorkerReadChangeFeed));
			} else {
				// FIXME: if we're already BlobWorkerReadChangeFeed, don't do a delay?
				wait(delay(0, TaskPriority::BlobWorkerReadChangeFeed));
			}

			state Standalone<VectorRef<MutationsAndVersionRef>> mutations;
			try {
				// Even if there are no new mutations, there still might be readers waiting on durableDeltaVersion
				// to advance. We need to check whether any outstanding files have finished so we don't wait on
				// mutations forever
				choose {
					when(Standalone<VectorRef<MutationsAndVersionRef>> _mutations =
					         waitNext(metadata->activeCFData.get()->mutations.getFuture())) {
						mutations = _mutations;
						ASSERT(!mutations.empty());
						if (readOldChangeFeed) {
							ASSERT(mutations.back().version < startState.changeFeedStartVersion);
						} else {
							ASSERT(mutations.front().version >= startState.changeFeedStartVersion);
						}

						if (mutations.front().version <= metadata->bufferedDeltaVersion) {
							fmt::print("ERROR: Mutations went backwards for granule [{0} - {1}). "
							           "bufferedDeltaVersion={2}, mutationVersion={3} !!!\n",
							           metadata->keyRange.begin.printable(),
							           metadata->keyRange.end.printable(),
							           metadata->bufferedDeltaVersion,
							           mutations.front().version);
						}
						ASSERT(mutations.front().version > metadata->bufferedDeltaVersion);

						// If this assert trips we should have gotten change_feed_popped from SS and didn't
						ASSERT(mutations.front().version >= metadata->activeCFData.get()->popVersion);
					}
					when(wait(inFlightFiles.empty() ? Never() : success(inFlightFiles.front().future))) {}
				}
			} catch (Error& e) {
				// only error we should expect here is when we finish consuming old change feed
				if (e.code() != error_code_end_of_stream) {
					throw;
				}
				ASSERT(readOldChangeFeed);

				readOldChangeFeed = false;
				// set this so next delta file write updates granule split metadata to done
				ASSERT(startState.parentGranule.present());
				oldChangeFeedDataComplete = startState.parentGranule.get();
				if (BW_DEBUG) {
					fmt::print("Granule [{0} - {1}) switching to new change feed {2} @ {3}\n",
					           metadata->keyRange.begin.printable(),
					           metadata->keyRange.end.printable(),
					           startState.granuleID.toString(),
					           metadata->bufferedDeltaVersion);
				}

				Reference<ChangeFeedData> cfData = makeReference<ChangeFeedData>();

				changeFeedFuture = bwData->db->getChangeFeedStream(cfData,
				                                                   cfKey,
				                                                   startState.changeFeedStartVersion,
				                                                   MAX_VERSION,
				                                                   metadata->keyRange,
				                                                   bwData->changeFeedStreamReplyBufferSize,
				                                                   false);

				// Start actors BEFORE setting new change feed data to ensure the change feed data is properly
				// initialized by the client
				metadata->activeCFData.set(cfData);
			}

			// process mutations
			if (!mutations.empty()) {
				bool processedAnyMutations = false;
				Version lastDeltaVersion = invalidVersion;
				for (MutationsAndVersionRef deltas : mutations) {

					// Buffer mutations at this version. There should not be multiple MutationsAndVersionRef with the
					// same version
					ASSERT(deltas.version > metadata->bufferedDeltaVersion);
					ASSERT(deltas.version > lastDeltaVersion);
					// FIXME: this assert isn't true - why
					// ASSERT(!deltas.mutations.empty());
					if (!deltas.mutations.empty()) {
						if (deltas.mutations.size() == 1 && deltas.mutations.back().param1 == lastEpochEndPrivateKey) {
							// Note rollbackVerision is durable, [rollbackVersion+1 - deltas.version] needs to be tossed
							// For correctness right now, there can be no waits and yields either in rollback handling
							// or in handleBlobGranuleFileRequest once waitForVersion has succeeded, otherwise this will
							// race and clobber results
							Version rollbackVersion;
							BinaryReader br(deltas.mutations[0].param2, Unversioned());
							br >> rollbackVersion;

							ASSERT(rollbackVersion >= metadata->durableDeltaVersion.get());

							if (!rollbacksInProgress.empty()) {
								ASSERT(rollbacksInProgress.front().first == rollbackVersion);
								ASSERT(rollbacksInProgress.front().second == deltas.version);
								if (BW_DEBUG) {
									fmt::print("Passed rollback {0} -> {1}\n", deltas.version, rollbackVersion);
								}
								rollbacksCompleted.push_back(rollbacksInProgress.front());
								rollbacksInProgress.pop_front();
							} else {
								// FIXME: add counter for granule rollbacks and rollbacks skipped?
								// explicitly check last delta in currentDeltas because lastVersion and
								// bufferedDeltaVersion include empties
								if (metadata->pendingDeltaVersion <= rollbackVersion &&
								    (metadata->currentDeltas.empty() ||
								     metadata->currentDeltas.back().version <= rollbackVersion)) {
									TEST(true); // Granule ignoring rollback

									if (BW_DEBUG) {
										fmt::print(
										    "Granule [{0} - {1}) on BW {2} skipping rollback {3} -> {4} completely\n",
										    metadata->keyRange.begin.printable().c_str(),
										    metadata->keyRange.end.printable().c_str(),
										    bwData->id.toString().substr(0, 5).c_str(),
										    deltas.version,
										    rollbackVersion);
									}
									// Still have to add to rollbacksCompleted. If we later roll the granule back past
									// this because of cancelling a delta file, we need to count this as in progress so
									// we can match the rollback mutation to a rollbackInProgress when we restart the
									// stream.
									rollbacksCompleted.push_back(std::pair(rollbackVersion, deltas.version));
								} else {
									TEST(true); // Granule processing rollback
									if (BW_DEBUG) {
										fmt::print("[{0} - {1}) on BW {2} ROLLBACK @ {3} -> {4}\n",
										           metadata->keyRange.begin.printable(),
										           metadata->keyRange.end.printable(),
										           bwData->id.toString().substr(0, 5).c_str(),
										           deltas.version,
										           rollbackVersion);
										TraceEvent(SevDebug, "GranuleRollback", bwData->id)
										    .detail("Granule", metadata->keyRange)
										    .detail("Version", deltas.version)
										    .detail("RollbackVersion", rollbackVersion);
									}

									Version cfRollbackVersion = doGranuleRollback(metadata,
									                                              deltas.version,
									                                              rollbackVersion,
									                                              inFlightFiles,
									                                              rollbacksInProgress,
									                                              rollbacksCompleted);

									Reference<ChangeFeedData> cfData = makeReference<ChangeFeedData>();

									if (!readOldChangeFeed && cfRollbackVersion < startState.changeFeedStartVersion) {
										// It isn't possible to roll back across the parent/child feed boundary, but as
										// part of rolling back we may need to cancel in-flight delta files, and those
										// delta files may include stuff from before the parent/child boundary. So we
										// have to go back to reading the old change feed
										ASSERT(cfRollbackVersion >= startState.previousDurableVersion);
										ASSERT(cfRollbackVersion >= metadata->durableDeltaVersion.get());
										TEST(true); // rollback crossed change feed boundaries
										readOldChangeFeed = true;
										oldChangeFeedDataComplete.reset();
									}

									if (readOldChangeFeed) {
										ASSERT(cfRollbackVersion < startState.changeFeedStartVersion);
										oldChangeFeedFuture =
										    bwData->db->getChangeFeedStream(cfData,
										                                    oldCFKey.get(),
										                                    cfRollbackVersion + 1,
										                                    startState.changeFeedStartVersion,
										                                    metadata->keyRange,
										                                    bwData->changeFeedStreamReplyBufferSize,
										                                    false);

									} else {
										if (cfRollbackVersion < startState.changeFeedStartVersion) {
											fmt::print("Rollback past CF start??. rollback={0}, start={1}\n",
											           cfRollbackVersion,
											           startState.changeFeedStartVersion);
										}
										ASSERT(cfRollbackVersion >= startState.changeFeedStartVersion);

										changeFeedFuture =
										    bwData->db->getChangeFeedStream(cfData,
										                                    cfKey,
										                                    cfRollbackVersion + 1,
										                                    MAX_VERSION,
										                                    metadata->keyRange,
										                                    bwData->changeFeedStreamReplyBufferSize,
										                                    false);
									}

									// Start actors BEFORE setting new change feed data to ensure the change feed data
									// is properly initialized by the client
									metadata->activeCFData.set(cfData);

									justDidRollback = true;
									break;
								}
							}
						} else if (!rollbacksInProgress.empty() && rollbacksInProgress.front().first < deltas.version &&
						           rollbacksInProgress.front().second > deltas.version) {
							TEST(true); // Granule skipping mutations b/c prior rollback
							if (BW_DEBUG) {
								fmt::print("Skipping mutations @ {} b/c prior rollback\n", deltas.version);
							}
						} else {
							for (auto& delta : deltas.mutations) {
								metadata->bufferedDeltaBytes += delta.totalSize();
								bwData->stats.changeFeedInputBytes += delta.totalSize();
								bwData->stats.mutationBytesBuffered += delta.totalSize();

								DEBUG_MUTATION("BlobWorkerBuffer", deltas.version, delta, bwData->id)
								    .detail("Granule", metadata->keyRange)
								    .detail("ChangeFeedID",
								            cfKeyToGranuleID(readOldChangeFeed ? oldCFKey.get() : cfKey))
								    .detail("OldChangeFeed", readOldChangeFeed ? "T" : "F");
							}
							metadata->currentDeltas.push_back_deep(metadata->currentDeltas.arena(), deltas);

							processedAnyMutations = true;
							ASSERT(deltas.version != invalidVersion);
							ASSERT(deltas.version > lastDeltaVersion);
							lastDeltaVersion = deltas.version;
						}
					}
					if (justDidRollback) {
						break;
					}
				}
				if (!justDidRollback && processedAnyMutations) {
					// update buffered version
					ASSERT(lastDeltaVersion != invalidVersion);
					ASSERT(lastDeltaVersion > metadata->bufferedDeltaVersion);

					// Update buffered delta version so new waitForVersion checks can bypass waiting entirely
					metadata->bufferedDeltaVersion = lastDeltaVersion;
				}
				justDidRollback = false;

				// Write a new delta file IF we have enough bytes
				if (metadata->bufferedDeltaBytes >= SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES) {
					if (BW_DEBUG) {
						fmt::print("Granule [{0} - {1}) flushing delta file after {2} bytes @ {3} {4}\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
						           metadata->bufferedDeltaBytes,
						           lastDeltaVersion,
						           oldChangeFeedDataComplete.present() ? ". Finalizing " : "");
					}
					TraceEvent(SevDebug, "BlobGranuleDeltaFile", bwData->id)
					    .detail("Granule", metadata->keyRange)
					    .detail("Version", lastDeltaVersion);

					// sanity check for version order
					ASSERT(lastDeltaVersion >= metadata->currentDeltas.back().version);
					ASSERT(metadata->pendingDeltaVersion < metadata->currentDeltas.front().version);

					// launch pipelined, but wait for previous operation to complete before persisting to FDB
					Future<BlobFileIndex> previousFuture;
					if (!inFlightFiles.empty()) {
						previousFuture = inFlightFiles.back().future;
					} else {
						previousFuture = Future<BlobFileIndex>(BlobFileIndex());
					}
					Future<BlobFileIndex> dfFuture =
					    writeDeltaFile(bwData,
					                   metadata->keyRange,
					                   startState.granuleID,
					                   metadata->originalEpoch,
					                   metadata->originalSeqno,
					                   metadata->currentDeltas,
					                   lastDeltaVersion,
					                   previousFuture,
					                   waitVersionCommitted(bwData, metadata, lastDeltaVersion),
					                   oldChangeFeedDataComplete);
					inFlightFiles.push_back(
					    InFlightFile(dfFuture, lastDeltaVersion, metadata->bufferedDeltaBytes, false));

					oldChangeFeedDataComplete.reset();
					// add new pending delta file
					ASSERT(metadata->pendingDeltaVersion < lastDeltaVersion);
					metadata->pendingDeltaVersion = lastDeltaVersion;
					metadata->bytesInNewDeltaFiles += metadata->bufferedDeltaBytes;

					bwData->stats.mutationBytesBuffered -= metadata->bufferedDeltaBytes;

					// reset current deltas
					metadata->currentDeltas = Standalone<GranuleDeltas>();
					metadata->bufferedDeltaBytes = 0;

					// if we just wrote a delta file, check if we need to compact here.
					// exhaust old change feed before compacting - otherwise we could end up with an endlessly
					// growing list of previous change feeds in the worst case.
					snapshotEligible = true;
				}

				// FIXME: if we're still reading from old change feed, we should probably compact if we're making a
				// bunch of extra delta files at some point, even if we don't consider it for a split yet

				// If we have enough delta files, try to re-snapshot
				if (snapshotEligible && metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT &&
				    metadata->pendingDeltaVersion >= startState.changeFeedStartVersion) {
					if (BW_DEBUG && !inFlightFiles.empty()) {
						fmt::print("Granule [{0} - {1}) ready to re-snapshot at {2} after {3} > {4} bytes, waiting for "
						           "outstanding {5} files to finish\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
						           metadata->pendingDeltaVersion,
						           metadata->bytesInNewDeltaFiles,
						           SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT,
						           inFlightFiles.size());
					}

					// Speculatively assume we will get the range back. This is both a performance optimization, and
					// necessary to keep consuming versions from the change feed so that we can realize
					// our last delta file is committed and write it

					Future<BlobFileIndex> previousFuture;
					if (!inFlightFiles.empty()) {
						previousFuture = inFlightFiles.back().future;
						ASSERT(!inFlightFiles.back().snapshot);
					} else {
						previousFuture = Future<BlobFileIndex>(metadata->files.deltaFiles.back());
					}
					int64_t versionsSinceLastSnapshot =
					    metadata->pendingDeltaVersion - metadata->pendingSnapshotVersion;
					Future<BlobFileIndex> inFlightBlobSnapshot = checkSplitAndReSnapshot(bwData,
					                                                                     metadata,
					                                                                     startState.granuleID,
					                                                                     metadata->bytesInNewDeltaFiles,
					                                                                     previousFuture,
					                                                                     versionsSinceLastSnapshot);
					inFlightFiles.push_back(InFlightFile(inFlightBlobSnapshot, metadata->pendingDeltaVersion, 0, true));
					pendingSnapshots++;

					metadata->pendingSnapshotVersion = metadata->pendingDeltaVersion;

					// reset metadata
					metadata->bytesInNewDeltaFiles = 0;

					// If we have more than one snapshot file and that file is unblocked (committedVersion >=
					// snapshotVersion), wait for it to finish

					if (pendingSnapshots > 1) {
						state int waitIdx = 0;
						int idx = 0;
						Version safeVersion =
						    std::max(metadata->knownCommittedVersion,
						             metadata->bufferedDeltaVersion - SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS);
						for (auto& f : inFlightFiles) {
							if (f.snapshot && f.version < metadata->pendingSnapshotVersion &&
							    f.version <= safeVersion) {
								if (BW_DEBUG) {
									fmt::print("[{0} - {1}) Waiting on previous snapshot file @ {2} <= {3}\n",
									           metadata->keyRange.begin.printable(),
									           metadata->keyRange.end.printable(),
									           f.version,
									           safeVersion);
								}
								waitIdx = idx + 1;
							}
							idx++;
						}
						while (waitIdx > 0) {
							TEST(true); // Granule blocking on previous snapshot
							// TODO don't duplicate code
							BlobFileIndex completedFile = wait(inFlightFiles.front().future);
							if (inFlightFiles.front().snapshot) {
								if (metadata->files.deltaFiles.empty()) {
									ASSERT(completedFile.version == metadata->initialSnapshotVersion);
								} else {
									ASSERT(completedFile.version == metadata->files.deltaFiles.back().version);
								}
								metadata->files.snapshotFiles.push_back(completedFile);
								metadata->durableSnapshotVersion.set(completedFile.version);
								pendingSnapshots--;
							} else {
								handleCompletedDeltaFile(bwData,
								                         metadata,
								                         completedFile,
								                         cfKey,
								                         startState.changeFeedStartVersion,
								                         &rollbacksCompleted);
							}

							inFlightFiles.pop_front();
							waitIdx--;
							wait(yield(TaskPriority::BlobWorkerUpdateStorage));
						}
					}

				} else if (snapshotEligible &&
				           metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
					// if we're in the old change feed case and can't snapshot but we have enough data to, don't
					// queue too many files in parallel, and slow down change feed consuming to let file writing
					// catch up

					TEST(true); // Granule processing long tail of old change feed
					if (inFlightFiles.size() > 10 && inFlightFiles.front().version <= metadata->knownCommittedVersion) {
						if (BW_DEBUG) {
							fmt::print("[{0} - {1}) Waiting on delta file b/c old change feed\n",
							           metadata->keyRange.begin.printable(),
							           metadata->keyRange.end.printable());
						}
						choose {
							when(BlobFileIndex completedDeltaFile = wait(inFlightFiles.front().future)) {}
							when(wait(delay(0.1))) {}
						}
					}
				}
				snapshotEligible = false;
			}
		}
	} catch (Error& e) {
		// Free last change feed data
		metadata->activeCFData.set(Reference<ChangeFeedData>());

		if (e.code() == error_code_operation_cancelled) {
			throw;
		}

		if (metadata->cancelled.canBeSet()) {
			metadata->cancelled.send(Void());
		}

		if (e.code() == error_code_granule_assignment_conflict) {
			TraceEvent("GranuleAssignmentConflict", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("GranuleID", startState.granuleID);
			return Void();
		}
		if (e.code() == error_code_change_feed_popped) {
			TraceEvent("GranuleChangeFeedPopped", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("GranuleID", startState.granuleID);
			return Void();
		}
		++bwData->stats.granuleUpdateErrors;
		if (BW_DEBUG) {
			fmt::print("Granule file updater for [{0} - {1}) got error {2}, exiting\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           e.name());
		}

		if (granuleCanRetry(e)) {
			TEST(true); // Granule close and re-open on error
			TraceEvent("GranuleFileUpdaterRetriableError", bwData->id)
			    .error(e)
			    .detail("Granule", metadata->keyRange)
			    .detail("GranuleID", startState.granuleID);
			// explicitly cancel all outstanding write futures BEFORE updating promise stream, to ensure they
			// can't update files after the re-assigned granule acquires the lock
			// do it backwards though because future depends on previous one, so it could cause a cascade
			for (int i = inFlightFiles.size() - 1; i >= 0; i--) {
				inFlightFiles[i].future.cancel();
			}

			// if we retry and re-open, we need to use a normal request (no continue) and update the
			// seqno
			metadata->originalReq.managerEpoch = metadata->continueEpoch;
			metadata->originalReq.managerSeqno = metadata->continueSeqno;
			metadata->originalReq.type = AssignRequestType::Normal;

			bwData->granuleUpdateErrors.send(metadata->originalReq);
			throw e;
		}

		TraceEvent(SevError, "GranuleFileUpdaterUnexpectedError", bwData->id)
		    .error(e)
		    .detail("Granule", metadata->keyRange)
		    .detail("GranuleID", startState.granuleID);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BW
		if (bwData->fatalError.canBeSet()) {
			bwData->fatalError.sendError(e);
		}
		throw e;
	}
}

// walk graph back to previous known version
// Once loaded, go reverse up stack inserting each into the graph and setting its parent pointer.
// if a racing granule already loaded a prefix of the history, skip inserting entries already present
ACTOR Future<Void> blobGranuleLoadHistory(Reference<BlobWorkerData> bwData,
                                          Reference<GranuleMetadata> metadata,
                                          Future<GranuleStartState> assignFuture) {
	try {
		GranuleStartState startState = wait(assignFuture);
		state Optional<GranuleHistory> activeHistory = startState.history;

		if (activeHistory.present() && activeHistory.get().value.parentGranules.size() > 0) {
			state Transaction tr(bwData->db);
			state GranuleHistory curHistory = activeHistory.get();
			ASSERT(activeHistory.get().value.parentGranules.size() == 1);

			state Version stopVersion;
			auto prev = bwData->granuleHistory.rangeContaining(metadata->keyRange.begin);
			// FIXME: not true for merges
			ASSERT(prev.begin() <= metadata->keyRange.begin && prev.end() >= metadata->keyRange.end);
			stopVersion = prev.value().isValid() ? prev.value()->startVersion : invalidVersion;

			state std::vector<Reference<GranuleHistoryEntry>> historyEntryStack;
			state bool foundHistory = true;

			// while the start version of the current granule's parent not past the last known start version,
			// walk backwards
			while (curHistory.value.parentGranules.size() > 0 &&
			       curHistory.value.parentGranules[0].second >= stopVersion) {
				state GranuleHistory next;

				loop {
					try {
						Optional<Value> v = wait(tr.get(blobGranuleHistoryKeyFor(
						    curHistory.value.parentGranules[0].first, curHistory.value.parentGranules[0].second)));
						if (!v.present()) {
							foundHistory = false;
						} else {
							next = GranuleHistory(curHistory.value.parentGranules[0].first,
							                      curHistory.value.parentGranules[0].second,
							                      decodeBlobGranuleHistoryValue(v.get()));
						}

						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
				if (!foundHistory) {
					break;
				}

				ASSERT(next.version != invalidVersion);
				// granule next.granuleID goes from the version range [next.version, curHistory.version]
				historyEntryStack.push_back(makeReference<GranuleHistoryEntry>(
				    next.range, next.value.granuleID, next.version, curHistory.version));
				curHistory = next;
			}

			if (!historyEntryStack.empty()) {
				Version oldestStartVersion = historyEntryStack.back()->startVersion;
				if (!foundHistory && stopVersion != invalidVersion) {
					stopVersion = oldestStartVersion;
				}
				ASSERT(stopVersion == oldestStartVersion || stopVersion == invalidVersion);
			} else {
				if (!foundHistory && stopVersion != invalidVersion) {
					stopVersion = invalidVersion;
				}
				ASSERT(stopVersion == invalidVersion);
			}

			// go back up stack and apply history entries from oldest to newest, skipping ranges that were already
			// applied by other racing loads.
			// yielding in this loop would mean we'd need to re-check for load races
			auto prev2 = bwData->granuleHistory.rangeContaining(metadata->keyRange.begin);
			// FIXME: not true for merges
			ASSERT(prev2.begin() <= metadata->keyRange.begin && prev2.end() >= metadata->keyRange.end);
			stopVersion = prev2.value().isValid() ? prev2.value()->startVersion : invalidVersion;

			int i = historyEntryStack.size() - 1;
			while (i >= 0 && historyEntryStack[i]->startVersion <= stopVersion) {
				TEST(true); // Granule skipping history entries loaded by parallel reader
				i--;
			}
			int skipped = historyEntryStack.size() - 1 - i;

			while (i >= 0) {
				auto prevRanges = bwData->granuleHistory.rangeContaining(historyEntryStack[i]->range.begin);

				if (prevRanges.value().isValid() &&
				    prevRanges.value()->endVersion != historyEntryStack[i]->startVersion) {
					historyEntryStack[i]->parentGranule = Reference<GranuleHistoryEntry>();
				} else {
					historyEntryStack[i]->parentGranule = prevRanges.value();
				}

				bwData->granuleHistory.insert(historyEntryStack[i]->range, historyEntryStack[i]);
				i--;
			}

			if (BW_DEBUG) {
				fmt::print("Loaded {0} history entries for granule [{1} - {2}) ({3} skipped)\n",
				           historyEntryStack.size(),
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable(),
				           skipped);
			}
		}

		metadata->historyLoaded.send(Void());
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		if (e.code() == error_code_granule_assignment_conflict) {
			return Void();
		}
		// SplitStorageMetrics explicitly has a SevError if it gets an error, so no errors should propagate here
		TraceEvent(SevError, "BlobWorkerUnexpectedErrorLoadGranuleHistory", bwData->id).error(e);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BW
		if (bwData->fatalError.canBeSet()) {
			bwData->fatalError.sendError(e);
		}
		throw e;
	}
}

// TODO might want to separate this out for valid values for range assignments vs read requests. Assignment conflict
// isn't valid for read requests but is for assignments
namespace {
bool canReplyWith(Error e) {
	switch (e.code()) {
	case error_code_blob_granule_transaction_too_old:
	case error_code_transaction_too_old:
	case error_code_future_version: // not thrown yet
	case error_code_wrong_shard_server:
	case error_code_process_behind: // not thrown yet
		return true;
	default:
		return false;
	};
}
} // namespace

// assumes metadata is already readable and the query is reading from the active granule, not a history one
ACTOR Future<Void> waitForVersion(Reference<GranuleMetadata> metadata, Version v) {
	// if we don't have to wait for change feed version to catch up or wait for any pending file writes to complete,
	// nothing to do

	if (BW_REQUEST_DEBUG) {
		fmt::print("WFV {0}) CF={1}, pendingD={2}, durableD={3}, pendingS={4}, durableS={5}\n",
		           v,
		           metadata->activeCFData.get()->getVersion(),
		           metadata->pendingDeltaVersion,
		           metadata->durableDeltaVersion.get(),
		           metadata->pendingSnapshotVersion,
		           metadata->durableSnapshotVersion.get());
	}

	ASSERT(metadata->activeCFData.get().isValid());

	if (v <= metadata->activeCFData.get()->getVersion() &&
	    (v <= metadata->durableDeltaVersion.get() ||
	     metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion) &&
	    (v <= metadata->durableSnapshotVersion.get() ||
	     metadata->durableSnapshotVersion.get() == metadata->pendingSnapshotVersion)) {
		TEST(true); // Granule read not waiting
		return Void();
	}

	// wait for change feed version to catch up to ensure we have all data
	if (metadata->activeCFData.get()->getVersion() < v) {
		wait(metadata->activeCFData.get()->whenAtLeast(v));
		ASSERT(metadata->activeCFData.get()->getVersion() >= v);
	}

	// wait for any pending delta and snapshot files as of the moment the change feed version caught up.
	state Version pendingDeltaV = metadata->pendingDeltaVersion;
	state Version pendingSnapshotV = metadata->pendingSnapshotVersion;

	// If there are mutations that are no longer buffered but have not been
	// persisted to a delta file that are necessary for the query, wait for them
	if (pendingDeltaV > metadata->durableDeltaVersion.get() && v > metadata->durableDeltaVersion.get()) {
		TEST(true); // Granule read waiting for pending delta
		wait(metadata->durableDeltaVersion.whenAtLeast(pendingDeltaV));
		ASSERT(metadata->durableDeltaVersion.get() >= pendingDeltaV);
	}

	// This isn't strictly needed, but if we're in the process of re-snapshotting, we'd likely rather
	// return that snapshot file than the previous snapshot file and all its delta files.
	if (pendingSnapshotV > metadata->durableSnapshotVersion.get() && v > metadata->durableSnapshotVersion.get()) {
		TEST(true); // Granule read waiting for pending snapshot
		wait(metadata->durableSnapshotVersion.whenAtLeast(pendingSnapshotV));
		ASSERT(metadata->durableSnapshotVersion.get() >= pendingSnapshotV);
	}

	// There is a race here - we wait for pending delta files before this to finish, but while we do, we
	// kick off another delta file and roll the mutations. In that case, we must return the new delta
	// file instead of in memory mutations, so we wait for that delta file to complete

	if (metadata->pendingDeltaVersion >= v) {
		TEST(true); // Granule mutations flushed while waiting for files to complete
		wait(metadata->durableDeltaVersion.whenAtLeast(v));
		ASSERT(metadata->durableDeltaVersion.get() >= v);
	}

	return Void();
}

ACTOR Future<Void> doBlobGranuleFileRequest(Reference<BlobWorkerData> bwData, BlobGranuleFileRequest req) {
	if (BW_REQUEST_DEBUG) {
		fmt::print("BW {0} processing blobGranuleFileRequest for range [{1} - {2}) @ ",
		           bwData->id.toString(),
		           req.keyRange.begin.printable(),
		           req.keyRange.end.printable(),
		           req.readVersion);
		if (req.beginVersion > 0) {
			fmt::print("{0} - {1}\n", req.beginVersion, req.readVersion);
		} else {
			fmt::print("{}", req.readVersion);
		}
	}

	state bool didCollapse = false;
	try {
		// TODO remove requirement for canCollapseBegin once we implement early replying
		ASSERT(req.beginVersion == 0 || req.canCollapseBegin);
		if (req.beginVersion != 0) {
			ASSERT(req.beginVersion > 0);
		}
		state BlobGranuleFileReply rep;
		state std::vector<Reference<GranuleMetadata>> granules;

		auto checkRanges = bwData->granuleMetadata.intersectingRanges(req.keyRange);
		// check for gaps as errors and copy references to granule metadata before yielding or doing any
		// work
		KeyRef lastRangeEnd = req.keyRange.begin;

		for (auto& r : checkRanges) {
			bool isValid = r.value().activeMetadata.isValid();
			if (lastRangeEnd < r.begin() || !isValid) {
				if (BW_REQUEST_DEBUG) {
					fmt::print("No {0} blob data for [{1} - {2}) in request range [{3} - {4}), skipping request\n",
					           isValid ? "" : "valid",
					           lastRangeEnd.printable(),
					           r.begin().printable(),
					           req.keyRange.begin.printable(),
					           req.keyRange.end.printable());
				}

				throw wrong_shard_server();
			}
			granules.push_back(r.value().activeMetadata);
			lastRangeEnd = r.end();
		}
		if (lastRangeEnd < req.keyRange.end) {
			if (BW_REQUEST_DEBUG) {
				fmt::print("No blob data for [{0} - {1}) in request range [{2} - {3}), skipping request\n",
				           lastRangeEnd.printable(),
				           req.keyRange.end.printable(),
				           req.keyRange.begin.printable(),
				           req.keyRange.end.printable());
			}

			throw wrong_shard_server();
		}

		// do work for each range
		state Key readThrough = req.keyRange.begin;
		for (auto m : granules) {
			if (readThrough >= m->keyRange.end) {
				// previous read did time travel that already included this granule
				// FIXME: this will get more complicated with merges where this could potentially
				// include partial boundaries. For now with only splits we can skip the whole range
				continue;
			}
			state Reference<GranuleMetadata> metadata = m;
			state Version granuleBeginVersion = req.beginVersion;

			choose {
				when(wait(metadata->readable.getFuture())) {}
				when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
			}

			// in case both readable and cancelled are ready, check cancelled
			if (!metadata->cancelled.canBeSet()) {
				throw wrong_shard_server();
			}

			state KeyRange chunkRange;
			state GranuleFiles chunkFiles;

			if (metadata->initialSnapshotVersion > req.readVersion) {
				TEST(true); // Granule Time Travel Read
				// this is a time travel query, find previous granule
				if (metadata->historyLoaded.canBeSet()) {
					choose {
						when(wait(metadata->historyLoaded.getFuture())) {}
						when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
					}
				}

				// FIXME: doesn't work once we add granule merging, could be multiple ranges and/or
				// multiple parents
				Key historySearchKey = std::max(req.keyRange.begin, metadata->keyRange.begin);
				Reference<GranuleHistoryEntry> cur = bwData->granuleHistory.rangeContaining(historySearchKey).value();

				// FIXME: use skip pointers here
				Version expectedEndVersion = metadata->initialSnapshotVersion;
				if (cur.isValid()) {
					ASSERT(cur->endVersion == expectedEndVersion);
				}
				while (cur.isValid() && req.readVersion < cur->startVersion) {
					// assert version of history is contiguous
					ASSERT(cur->endVersion == expectedEndVersion);
					expectedEndVersion = cur->startVersion;
					cur = cur->parentGranule;
				}

				if (!cur.isValid()) {
					// this request predates blob data
					throw blob_granule_transaction_too_old();
				}

				if (BW_REQUEST_DEBUG) {
					fmt::print("[{0} - {1}) @ {2} time traveled back to {3} [{4} - {5}) @ [{6} - {7})\n",
					           req.keyRange.begin.printable(),
					           req.keyRange.end.printable(),
					           req.readVersion,
					           cur->granuleID.toString(),
					           cur->range.begin.printable(),
					           cur->range.end.printable(),
					           cur->startVersion,
					           cur->endVersion);
				}

				ASSERT(cur->endVersion > req.readVersion);
				ASSERT(cur->startVersion <= req.readVersion);

				// lazily load files for old granule if not present
				chunkRange = cur->range;
				if (!cur->files.isValid() || cur->files.isError()) {
					cur->files = loadHistoryFiles(bwData->db, cur->granuleID);
				}

				choose {
					when(GranuleFiles _f = wait(cur->files)) { chunkFiles = _f; }
					when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
				}

				if (chunkFiles.snapshotFiles.empty()) {
					// a snapshot file must have been pruned
					throw blob_granule_transaction_too_old();
				}

				ASSERT(!chunkFiles.deltaFiles.empty());
				ASSERT(chunkFiles.deltaFiles.back().version > req.readVersion);
				if (chunkFiles.snapshotFiles.front().version > req.readVersion) {
					// a snapshot file must have been pruned
					throw blob_granule_transaction_too_old();
				}
			} else {
				TEST(true); // Granule Active Read
				// this is an active granule query
				loop {
					if (!metadata->activeCFData.get().isValid() || !metadata->cancelled.canBeSet()) {
						throw wrong_shard_server();
					}
					Future<Void> waitForVersionFuture = waitForVersion(metadata, req.readVersion);
					if (waitForVersionFuture.isReady()) {
						// didn't wait, so no need to check rollback stuff
						break;
					}
					// rollback resets all of the version information, so we have to redo wait for
					// version on rollback
					try {
						choose {
							when(wait(waitForVersionFuture)) { break; }
							when(wait(metadata->activeCFData.onChange())) {}
							when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
						}
					} catch (Error& e) {
						// We can get change feed cancelled from whenAtLeast. This means the change feed may retry, or
						// may be cancelled. Wait a bit and try again to see
						if (e.code() == error_code_change_feed_popped) {
							TEST(true); // Change feed popped while read waiting
							throw wrong_shard_server();
						}
						if (e.code() != error_code_change_feed_cancelled) {
							throw e;
						}
						TEST(true); // Change feed switched while read waiting
						// wait 1ms and try again
						wait(delay(0.001));
					}
					if ((BW_REQUEST_DEBUG) && metadata->activeCFData.get().isValid()) {
						fmt::print("{0} - {1}) @ {2} hit CF change, restarting waitForVersion\n",
						           req.keyRange.begin.printable().c_str(),
						           req.keyRange.end.printable().c_str(),
						           req.readVersion);
					}
				}
				chunkFiles = metadata->files;
				chunkRange = metadata->keyRange;
			}

			if (!metadata->cancelled.canBeSet()) {
				fmt::print("ERROR: Request [{0} - {1}) @ {2} cancelled for granule [{3} - {4}) after waitForVersion!\n",
				           req.keyRange.begin.printable(),
				           req.keyRange.end.printable(),
				           req.readVersion,
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable());
			}

			// granule is up to date, do read
			ASSERT(metadata->cancelled.canBeSet());

			// Right now we force a collapse if the version range crosses granule boundaries, for simplicity
			if (granuleBeginVersion <= chunkFiles.snapshotFiles.front().version) {
				TEST(true); // collapsed begin version request because of boundaries
				didCollapse = true;
				granuleBeginVersion = 0;
			}
			BlobGranuleChunkRef chunk;
			// TODO change with early reply
			chunk.includedVersion = req.readVersion;
			chunk.keyRange = KeyRangeRef(StringRef(rep.arena, chunkRange.begin), StringRef(rep.arena, chunkRange.end));

			int64_t deltaBytes = 0;
			chunkFiles.getFiles(
			    granuleBeginVersion, req.readVersion, req.canCollapseBegin, chunk, rep.arena, deltaBytes);
			bwData->stats.readReqDeltaBytesReturned += deltaBytes;
			if (granuleBeginVersion > 0 && chunk.snapshotFile.present()) {
				TEST(true); // collapsed begin version request for efficiency
				didCollapse = true;
			}

			// new deltas (if version is larger than version of last delta file)
			// FIXME: do trivial key bounds here if key range is not fully contained in request key
			// range
			if (req.readVersion > metadata->durableDeltaVersion.get() && !metadata->currentDeltas.empty()) {
				if (metadata->durableDeltaVersion.get() != metadata->pendingDeltaVersion) {
					fmt::print("real-time read [{0} - {1}) @ {2} doesn't have mutations!! durable={3}, pending={4}\n",
					           metadata->keyRange.begin.printable(),
					           metadata->keyRange.end.printable(),
					           req.readVersion,
					           metadata->durableDeltaVersion.get(),
					           metadata->pendingDeltaVersion);
				}

				// prune mutations based on begin version, if possible
				ASSERT(metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion);
				// FIXME: I think we can remove this dependsOn since we are doing push_back_deep
				rep.arena.dependsOn(metadata->currentDeltas.arena());
				MutationsAndVersionRef* mutationIt = metadata->currentDeltas.begin();
				if (granuleBeginVersion > metadata->currentDeltas.back().version) {
					TEST(true); // beginVersion pruning all in-memory mutations
					mutationIt = metadata->currentDeltas.end();
				} else if (granuleBeginVersion > metadata->currentDeltas.front().version) {
					// binary search for beginVersion
					TEST(true); // beginVersion pruning some in-memory mutations
					mutationIt = std::lower_bound(metadata->currentDeltas.begin(),
					                              metadata->currentDeltas.end(),
					                              MutationsAndVersionRef(granuleBeginVersion, 0),
					                              MutationsAndVersionRef::OrderByVersion());
				}

				// add mutations to response
				while (mutationIt != metadata->currentDeltas.end()) {
					if (mutationIt->version > req.readVersion) {
						TEST(true); // readVersion pruning some in-memory mutations
						break;
					}
					chunk.newDeltas.push_back_deep(rep.arena, *mutationIt);
					mutationIt++;
				}
			}

			rep.chunks.push_back(rep.arena, chunk);

			bwData->stats.readReqTotalFilesReturned += chunk.deltaFiles.size() + int(chunk.snapshotFile.present());
			readThrough = chunk.keyRange.end;

			wait(yield(TaskPriority::DefaultEndpoint));
		}
		// do these together to keep them synchronous
		if (req.beginVersion != 0) {
			++bwData->stats.readRequestsWithBegin;
		}
		if (didCollapse) {
			++bwData->stats.readRequestsCollapsed;
		}
		ASSERT(!req.reply.isSet());
		req.reply.send(rep);
		--bwData->stats.activeReadRequests;
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			req.reply.sendError(wrong_shard_server());
			throw;
		}

		if (e.code() == error_code_wrong_shard_server) {
			++bwData->stats.wrongShardServer;
		}
		--bwData->stats.activeReadRequests;
		if (canReplyWith(e)) {
			req.reply.sendError(e);
		} else {
			throw e;
		}
	}
	return Void();
}

ACTOR Future<Void> handleBlobGranuleFileRequest(Reference<BlobWorkerData> bwData, BlobGranuleFileRequest req) {
	choose {
		when(wait(doBlobGranuleFileRequest(bwData, req))) {}
		when(wait(delay(SERVER_KNOBS->BLOB_WORKER_REQUEST_TIMEOUT))) {
			if (!req.reply.isSet()) {
				TEST(true); // Blob Worker request timeout hit
				if (BW_DEBUG) {
					fmt::print("BW {0} request [{1} - {2}) @ {3} timed out, sending WSS\n",
					           bwData->id.toString().substr(0, 5),
					           req.keyRange.begin.printable(),
					           req.keyRange.end.printable(),
					           req.readVersion);
				}
				--bwData->stats.activeReadRequests;
				++bwData->stats.granuleRequestTimeouts;

				// return wrong_shard_server because it's possible that someone else actually owns the granule now
				req.reply.sendError(wrong_shard_server());
			}
		}
	}
	return Void();
}

// FIXME: move this up by other granule state stuff like BGUF
ACTOR Future<GranuleStartState> openGranule(Reference<BlobWorkerData> bwData, AssignBlobRangeRequest req) {
	ASSERT(req.type != AssignRequestType::Continue);
	state Transaction tr(bwData->db);
	state Key lockKey = blobGranuleLockKeyFor(req.keyRange);
	state UID newGranuleID = deterministicRandom()->randomUniqueID();

	if (BW_DEBUG) {
		fmt::print("{0} [{1} - {2}) opening\n",
		           bwData->id.toString(),
		           req.keyRange.begin.printable(),
		           req.keyRange.end.printable());
	}

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			state GranuleStartState info;
			info.changeFeedStartVersion = invalidVersion;

			state Future<Optional<Value>> fLockValue = tr.get(lockKey);
			Future<Optional<GranuleHistory>> fHistory = getLatestGranuleHistory(&tr, req.keyRange);
			Optional<GranuleHistory> history = wait(fHistory);
			info.history = history;
			Optional<Value> prevLockValue = wait(fLockValue);
			state bool hasPrevOwner = prevLockValue.present();
			if (hasPrevOwner) {
				TEST(true); // Granule open found previous owner
				std::tuple<int64_t, int64_t, UID> prevOwner = decodeBlobGranuleLockValue(prevLockValue.get());
				acquireGranuleLock(req.managerEpoch, req.managerSeqno, std::get<0>(prevOwner), std::get<1>(prevOwner));
				info.granuleID = std::get<2>(prevOwner);

				// if it's the first snapshot of a new granule, history won't be present
				if (info.history.present()) {
					ASSERT(info.granuleID == info.history.get().value.granuleID);
				}

				GranuleFiles granuleFiles = wait(loadPreviousFiles(&tr, info.granuleID));
				info.existingFiles = granuleFiles;
				info.doSnapshot = false;

				if (!info.history.present()) {
					// the only time history can be not present if a lock already exists is if it's a
					// new granule and it died before it could persist the initial snapshot from FDB
					ASSERT(info.existingFiles.get().snapshotFiles.empty());
				}

				if (info.existingFiles.get().snapshotFiles.empty()) {
					ASSERT(info.existingFiles.get().deltaFiles.empty());
					info.previousDurableVersion = invalidVersion;
					info.doSnapshot = true;
				} else if (info.existingFiles.get().deltaFiles.empty()) {
					info.previousDurableVersion = info.existingFiles.get().snapshotFiles.back().version;
				} else {
					info.previousDurableVersion = info.existingFiles.get().deltaFiles.back().version;
				}

				// for the non-splitting cases, this doesn't need to be 100% accurate, it just needs to
				// be smaller than the next delta file write.
				info.changeFeedStartVersion = info.previousDurableVersion;
			} else {
				// else we are first, no need to check for owner conflict
				if (info.history.present()) {
					// if this granule is derived from a split or merge, this history entry is already
					// present (written by the blob manager)
					info.granuleID = info.history.get().value.granuleID;
				} else {
					// FIXME: could avoid max uid for granule ids here
					// if this granule is not derived from a split or merge, use new granule id
					info.granuleID = newGranuleID;
				}
				wait(updateChangeFeed(
				    &tr, granuleIDToCFKey(info.granuleID), ChangeFeedStatus::CHANGE_FEED_CREATE, req.keyRange));
				info.doSnapshot = true;
				info.previousDurableVersion = invalidVersion;
			}

			tr.set(lockKey, blobGranuleLockValueFor(req.managerEpoch, req.managerSeqno, info.granuleID));
			wait(krmSetRange(&tr, blobGranuleMappingKeys.begin, req.keyRange, blobGranuleMappingValueFor(bwData->id)));

			// If anything in previousGranules, need to do the handoff logic and set
			// ret.previousChangeFeedId, and the previous durable version will come from the previous
			// granules
			if (info.history.present() && info.history.get().value.parentGranules.size() > 0) {
				TEST(true); // Granule open found parent
				// TODO change this for merge
				ASSERT(info.history.get().value.parentGranules.size() == 1);
				state KeyRange parentGranuleRange = info.history.get().value.parentGranules[0].first;

				Optional<Value> parentGranuleLockValue = wait(tr.get(blobGranuleLockKeyFor(parentGranuleRange)));
				if (parentGranuleLockValue.present()) {
					std::tuple<int64_t, int64_t, UID> parentGranuleLock =
					    decodeBlobGranuleLockValue(parentGranuleLockValue.get());
					UID parentGranuleID = std::get<2>(parentGranuleLock);
					if (BW_DEBUG) {
						fmt::print("  parent granule id {}\n", parentGranuleID.toString());
					}

					info.parentGranule = std::pair(parentGranuleRange, parentGranuleID);

					state std::pair<BlobGranuleSplitState, Version> granuleSplitState =
					    std::pair(BlobGranuleSplitState::Initialized, invalidVersion);
					if (hasPrevOwner) {
						std::pair<BlobGranuleSplitState, Version> _gss =
						    wait(getGranuleSplitState(&tr, parentGranuleID, info.granuleID));
						granuleSplitState = _gss;
					}

					if (granuleSplitState.first == BlobGranuleSplitState::Assigned) {
						TEST(true); // Granule open found granule in assign state
						// was already assigned, use change feed start version
						ASSERT(granuleSplitState.second > 0);
						info.changeFeedStartVersion = granuleSplitState.second;
					} else if (granuleSplitState.first == BlobGranuleSplitState::Initialized) {
						TEST(true); // Granule open found granule in initialized state
						wait(updateGranuleSplitState(&tr,
						                             info.parentGranule.get().first,
						                             info.parentGranule.get().second,
						                             info.granuleID,
						                             BlobGranuleSplitState::Assigned));
						// change feed was created as part of this transaction, changeFeedStartVersion
						// will be set later
					} else {
						TEST(true); // Granule open found granule in done state
						// this sub-granule is done splitting, no need for split logic.
						info.parentGranule.reset();
					}
				}

				if (info.doSnapshot) {
					ASSERT(info.parentGranule.present());
					// only need to do snapshot if no files exist yet for this granule.
					ASSERT(info.previousDurableVersion == invalidVersion);
					GranuleFiles prevFiles = wait(loadPreviousFiles(&tr, info.parentGranule.get().second));
					ASSERT(!prevFiles.snapshotFiles.empty() || !prevFiles.deltaFiles.empty());

					info.blobFilesToSnapshot = prevFiles;
					info.previousDurableVersion = info.blobFilesToSnapshot.get().deltaFiles.empty()
					                                  ? info.blobFilesToSnapshot.get().snapshotFiles.back().version
					                                  : info.blobFilesToSnapshot.get().deltaFiles.back().version;
				}
			}
			wait(tr.commit());

			if (info.changeFeedStartVersion == invalidVersion) {
				info.changeFeedStartVersion = tr.getCommittedVersion();
			}

			TraceEvent openEv("GranuleOpen", bwData->id);
			openEv.detail("GranuleID", info.granuleID)
			    .detail("Granule", req.keyRange)
			    .detail("Epoch", req.managerEpoch)
			    .detail("Seqno", req.managerSeqno)
			    .detail("CFStartVersion", info.changeFeedStartVersion)
			    .detail("PreviousDurableVersion", info.previousDurableVersion);
			if (info.parentGranule.present()) {
				openEv.detail("ParentGranuleID", info.parentGranule.get().second);
			}

			return info;
		} catch (Error& e) {
			if (e.code() == error_code_granule_assignment_conflict) {
				throw e;
			}
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> start(Reference<BlobWorkerData> bwData, GranuleRangeMetadata* meta, AssignBlobRangeRequest req) {
	ASSERT(meta->activeMetadata.isValid());
	meta->activeMetadata->originalReq = req;
	meta->assignFuture = openGranule(bwData, req);
	meta->fileUpdaterFuture = blobGranuleUpdateFiles(bwData, meta->activeMetadata, meta->assignFuture);
	meta->historyLoaderFuture = blobGranuleLoadHistory(bwData, meta->activeMetadata, meta->assignFuture);
	wait(success(meta->assignFuture));
	return Void();
}

static GranuleRangeMetadata constructActiveBlobRange(Reference<BlobWorkerData> bwData,
                                                     KeyRange keyRange,
                                                     int64_t epoch,
                                                     int64_t seqno) {

	Reference<GranuleMetadata> newMetadata = makeReference<GranuleMetadata>();
	newMetadata->keyRange = keyRange;
	// FIXME: original Epoch/Seqno is now not necessary with originalReq
	newMetadata->originalEpoch = epoch;
	newMetadata->originalSeqno = seqno;
	newMetadata->continueEpoch = epoch;
	newMetadata->continueSeqno = seqno;

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
// If a key range [A, B) was assigned to the worker at seqno S1, no part of the keyspace that intersects
// [A, B] may be re-assigned to the worker until the range has been revoked from this worker. This
// revoking can either happen by the blob manager willingly relinquishing the range, or by the blob
// manager reassigning it somewhere else. This means that if the worker gets an assignment for any range
// that intersects [A, B) at S3, there must have been a revoke message for [A, B) with seqno S3 where S1
// < S2 < S3, that was delivered out of order. This means that if there are any intersecting but not
// fully overlapping ranges with a new range assignment, they had already been revoked. So the worker
// will mark them as revoked, but leave the sequence number as S1, so that when the actual revoke
// message comes in, it is a no-op, but updates the sequence number. Similarly, if a worker gets an
// assign message for any range that already has a higher sequence number, that range was either
// revoked, or revoked and then re-assigned. Either way, this assignment is no longer valid.

// Returns future to wait on to ensure prior work of other granules is done before responding to the
// manager with a successful assignment And if the change produced a new granule that needs to start
// doing work, returns the new granule so that the caller can start() it with the appropriate starting
// state.

// Not an actor because we need to guarantee it changes the synchronously as part of the request
static bool changeBlobRange(Reference<BlobWorkerData> bwData,
                            KeyRange keyRange,
                            int64_t epoch,
                            int64_t seqno,
                            bool active,
                            bool disposeOnCleanup,
                            bool selfReassign,
                            std::vector<Future<Void>>& toWaitOut,
                            Optional<AssignRequestType> assignType = Optional<AssignRequestType>()) {
	ASSERT(active == assignType.present());

	if (BW_DEBUG) {
		fmt::print("{0} range for [{1} - {2}): {3} @ ({4}, {5})\n",
		           selfReassign ? "Re-assigning" : "Changing",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           active ? "T" : "F",
		           epoch,
		           seqno);
	}

	// For each range that intersects this update:
	// If the identical range already exists at the same assignment sequence number and it is not a
	// self-reassign, this is a noop. Otherwise, this will consist of a series of ranges that are either
	// older, or newer. For each older range, cancel it if it is active. Insert the current range.
	// Re-insert all newer ranges over the current range.

	std::vector<std::pair<KeyRange, GranuleRangeMetadata>> newerRanges;

	auto ranges = bwData->granuleMetadata.intersectingRanges(keyRange);
	bool alreadyAssigned = false;
	for (auto& r : ranges) {
		bool thisAssignmentNewer = newerRangeAssignment(r.value(), epoch, seqno);
		if (BW_DEBUG) {
			fmt::print("thisAssignmentNewer={}\n", thisAssignmentNewer ? "true" : "false");
		}

		if (BW_DEBUG) {
			fmt::print("last: ({0}, {1}). now: ({2}, {3})\n", r.value().lastEpoch, r.value().lastSeqno, epoch, seqno);
		}

		if (r.value().lastEpoch == epoch && r.value().lastSeqno == seqno) {
			// the range in our map can be different if later the range was split, but then an old request gets retried.
			// Assume that it's the same as initially

			if (selfReassign) {
				thisAssignmentNewer = true;
			} else {
				if (BW_DEBUG) {
					printf("same assignment\n");
				}
				// applied the same assignment twice, make idempotent
				if (r.value().activeMetadata.isValid()) {
					toWaitOut.push_back(success(r.value().assignFuture));
				}
				alreadyAssigned = true;
				break;
			}
		}

		if (r.value().activeMetadata.isValid() && thisAssignmentNewer) {
			// cancel actors for old range and clear reference
			if (BW_DEBUG) {
				fmt::print("  [{0} - {1}): @ ({2}, {3}) (cancelling)\n",
				           r.begin().printable(),
				           r.end().printable(),
				           r.value().lastEpoch,
				           r.value().lastSeqno);
			}
			if (!active) {
				bwData->stats.numRangesAssigned--;
			}
			r.value().cancel();
		} else if (!thisAssignmentNewer) {
			// re-insert the known newer range over this existing range
			newerRanges.push_back(std::pair(r.range(), r.value()));
		}
	}

	if (alreadyAssigned) {
		return false;
	}

	// if range is active, and isn't surpassed by a newer range already, insert an active range
	GranuleRangeMetadata newMetadata = (active && newerRanges.empty())
	                                       ? constructActiveBlobRange(bwData, keyRange, epoch, seqno)
	                                       : constructInactiveBlobRange(epoch, seqno);

	bwData->granuleMetadata.insert(keyRange, newMetadata);
	if (BW_DEBUG) {
		fmt::print("Inserting new range [{0} - {1}): {2} @ ({3}, {4})\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           newMetadata.activeMetadata.isValid() ? "T" : "F",
		           newMetadata.lastEpoch,
		           newMetadata.lastSeqno);
	}

	for (auto& it : newerRanges) {
		if (BW_DEBUG) {
			fmt::print("Re-inserting newer range [{0} - {1}): {2} @ ({3}, {4})\n",
			           it.first.begin.printable(),
			           it.first.end.printable(),
			           it.second.activeMetadata.isValid() ? "T" : "F",
			           it.second.lastEpoch,
			           it.second.lastSeqno);
		}
		bwData->granuleMetadata.insert(it.first, it.second);
	}

	return newerRanges.size() == 0;
}

static bool resumeBlobRange(Reference<BlobWorkerData> bwData, KeyRange keyRange, int64_t epoch, int64_t seqno) {
	auto existingRange = bwData->granuleMetadata.rangeContaining(keyRange.begin);
	// if range boundaries don't match, or this (epoch, seqno) is old or the granule is inactive, ignore
	if (keyRange.begin != existingRange.begin() || keyRange.end != existingRange.end() ||
	    existingRange.value().lastEpoch > epoch ||
	    (existingRange.value().lastEpoch == epoch && existingRange.value().lastSeqno > seqno) ||
	    !existingRange.value().activeMetadata.isValid()) {

		if (BW_DEBUG) {
			fmt::print(
			    "BW {0} got out of date resume range for [{1} - {2}) @ ({3}, {4}). Currently  [{5} - {6}) @ ({7}, "
			    "{8}): {9}\n",
			    bwData->id.toString(),
			    existingRange.begin().printable(),
			    existingRange.end().printable(),
			    existingRange.value().lastEpoch,
			    existingRange.value().lastSeqno,
			    keyRange.begin.printable(),
			    keyRange.end.printable(),
			    epoch,
			    seqno,
			    existingRange.value().activeMetadata.isValid() ? "T" : "F");
		}

		return false;
	}
	if (existingRange.value().lastEpoch != epoch || existingRange.value().lastSeqno != seqno) {
		// update the granule metadata map, and the continueEpoch/seqno.  Saves an extra transaction
		existingRange.value().lastEpoch = epoch;
		existingRange.value().lastSeqno = seqno;
		existingRange.value().activeMetadata->continueEpoch = epoch;
		existingRange.value().activeMetadata->continueSeqno = seqno;
		existingRange.value().activeMetadata->resume();
	}
	// else we already processed this continue, do nothing
	return true;
}

// the contract of handleRangeAssign and handleRangeRevoke is that they change the mapping before doing any waiting.
// This ensures GetGranuleAssignment returns an up-to-date set of ranges
ACTOR Future<Void> handleRangeAssign(Reference<BlobWorkerData> bwData,
                                     AssignBlobRangeRequest req,
                                     bool isSelfReassign) {
	try {
		if (req.type == AssignRequestType::Continue) {
			resumeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno);
		} else {
			std::vector<Future<Void>> toWait;
			state bool shouldStart = changeBlobRange(bwData,
			                                         req.keyRange,
			                                         req.managerEpoch,
			                                         req.managerSeqno,
			                                         true,
			                                         false,
			                                         isSelfReassign,
			                                         toWait,
			                                         req.type);
			wait(waitForAll(toWait));

			if (shouldStart) {
				bwData->stats.numRangesAssigned++;
				auto m = bwData->granuleMetadata.rangeContaining(req.keyRange.begin);
				ASSERT(m.begin() == req.keyRange.begin && m.end() == req.keyRange.end);
				if (m.value().activeMetadata.isValid()) {
					wait(start(bwData, &m.value(), req));
				}
			}
		}
		if (!isSelfReassign) {
			ASSERT(!req.reply.isSet());
			req.reply.send(Void());
		}
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("AssignRange [{0} - {1}) ({2}, {3}) in BW {4} got error {5}\n",
			           req.keyRange.begin.printable().c_str(),
			           req.keyRange.end.printable().c_str(),
			           req.managerEpoch,
			           req.managerSeqno,
			           bwData->id.toString().c_str(),
			           e.name());
		}

		if (!isSelfReassign) {
			if (e.code() == error_code_granule_assignment_conflict) {
				req.reply.sendError(e);
				bwData->stats.numRangesAssigned--;
				return Void();
			}

			if (canReplyWith(e)) {
				req.reply.sendError(e);
			}
		}

		TraceEvent(SevError, "BlobWorkerUnexpectedErrorRangeAssign", bwData->id)
		    .error(e)
		    .detail("Range", req.keyRange)
		    .detail("ManagerEpoch", req.managerEpoch)
		    .detail("SeqNo", req.managerSeqno);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BW
		if (bwData->fatalError.canBeSet()) {
			bwData->fatalError.sendError(e);
		}
		throw e;
	}
}

ACTOR Future<Void> handleRangeRevoke(Reference<BlobWorkerData> bwData, RevokeBlobRangeRequest req) {
	try {
		std::vector<Future<Void>> toWait;
		changeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno, false, req.dispose, false, toWait);
		wait(waitForAll(toWait));
		req.reply.send(Void());
		return Void();
	} catch (Error& e) {
		// FIXME: retry on error if dispose fails?
		if (BW_DEBUG) {
			fmt::print("RevokeRange [{0} - {1}) ({2}, {3}) got error {4}\n",
			           req.keyRange.begin.printable(),
			           req.keyRange.end.printable(),
			           req.managerEpoch,
			           req.managerSeqno,
			           e.name());
		}
		if (canReplyWith(e)) {
			req.reply.sendError(e);
		}
		throw;
	}
}

ACTOR Future<Void> registerBlobWorker(Reference<BlobWorkerData> bwData, BlobWorkerInterface interf) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	TraceEvent("BlobWorkerRegister", bwData->id);
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		try {
			Key blobWorkerListKey = blobWorkerListKeyFor(interf.id());
			// FIXME: should be able to remove this conflict range
			tr->addReadConflictRange(singleKeyRange(blobWorkerListKey));
			tr->set(blobWorkerListKey, blobWorkerListValue(interf));

			// Get manager lock from DB
			Optional<Value> currentLockValue = wait(tr->get(blobManagerEpochKey));
			ASSERT(currentLockValue.present());
			int64_t currentEpoch = decodeBlobManagerEpochValue(currentLockValue.get());
			bwData->managerEpochOk(currentEpoch);

			wait(tr->commit());

			if (BW_DEBUG) {
				fmt::print("Registered blob worker {}\n", interf.id().toString());
			}
			TraceEvent("BlobWorkerRegistered", bwData->id);
			return Void();
		} catch (Error& e) {
			if (BW_DEBUG) {
				fmt::print("Registering blob worker {0} got error {1}\n", interf.id().toString(), e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> monitorRemoval(Reference<BlobWorkerData> bwData) {
	state Key blobWorkerListKey = blobWorkerListKeyFor(bwData->id);
	loop {
		loop {
			state ReadYourWritesTransaction tr(bwData->db);
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				Optional<Value> val = wait(tr.get(blobWorkerListKey));
				if (!val.present()) {
					TEST(true); // Blob worker found out BM killed it from reading DB
					return Void();
				}

				state Future<Void> watchFuture = tr.watch(blobWorkerListKey);

				wait(tr.commit());
				wait(watchFuture);
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

// Because change feeds send uncommitted data and explicit rollback messages, we speculatively buffer/write
// uncommitted data. This means we must ensure the data is actually committed before "committing" those writes in
// the blob granule. The simplest way to do this is to have the blob worker do a periodic GRV, which is guaranteed
// to be an earlier committed version. Then, once the change feed has consumed up through the GRV's data, we can
// guarantee nothing will roll back the in-memory mutations
ACTOR Future<Void> runGRVChecks(Reference<BlobWorkerData> bwData) {
	state Transaction tr(bwData->db);
	loop {
		// only do grvs to get committed version if we need it to persist delta files
		while (bwData->grvVersion.numWaiting() == 0) {
			wait(bwData->doGRVCheck.getFuture());
			bwData->doGRVCheck = Promise<Void>();
		}

		// batch potentially multiple delta files into one GRV, and also rate limit GRVs for this worker
		wait(delay(SERVER_KNOBS->BLOB_WORKER_BATCH_GRV_INTERVAL));

		tr.reset();
		try {
			Version readVersion = wait(tr.getReadVersion());
			ASSERT(readVersion >= bwData->grvVersion.get());
			bwData->grvVersion.set(readVersion);

			++bwData->stats.commitVersionChecks;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

static void handleGetGranuleAssignmentsRequest(Reference<BlobWorkerData> self,
                                               const GetGranuleAssignmentsRequest& req) {
	GetGranuleAssignmentsReply reply;
	auto allRanges = self->granuleMetadata.intersectingRanges(normalKeys);
	for (auto& it : allRanges) {
		if (it.value().activeMetadata.isValid()) {
			// range is active, copy into reply's arena
			StringRef start = StringRef(reply.arena, it.begin());
			StringRef end = StringRef(reply.arena, it.end());

			reply.assignments.push_back(
			    reply.arena, GranuleAssignmentRef(KeyRangeRef(start, end), it.value().lastEpoch, it.value().lastSeqno));
		}
	}
	if (BW_DEBUG) {
		fmt::print("Worker {0} sending {1} granule assignments back to BM {2}\n",
		           self->id.toString(),
		           reply.assignments.size(),
		           req.managerEpoch);
	}
	req.reply.send(reply);
}

ACTOR Future<Void> blobWorker(BlobWorkerInterface bwInterf,
                              ReplyPromise<InitializeBlobWorkerReply> recruitReply,
                              Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state Reference<BlobWorkerData> self(
	    new BlobWorkerData(bwInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)));
	self->id = bwInterf.id();
	self->locality = bwInterf.locality;

	state Future<Void> collection = actorCollection(self->addActor.getFuture());

	if (BW_DEBUG) {
		printf("Initializing blob worker s3 stuff\n");
	}

	try {
		if (BW_DEBUG) {
			fmt::print("BW constructing backup container from {0}\n", SERVER_KNOBS->BG_URL);
		}
		self->bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL, {}, {});
		if (BW_DEBUG) {
			printf("BW constructed backup container\n");
		}

		// register the blob worker to the system keyspace
		wait(registerBlobWorker(self, bwInterf));
	} catch (Error& e) {
		if (BW_DEBUG) {
			fmt::print("BW got backup container init error {0}\n", e.name());
		}
		// if any errors came up while initializing the blob worker, let the blob manager know
		// that recruitment failed
		if (!recruitReply.isSet()) {
			recruitReply.sendError(recruitment_failed());
		}
		throw e;
	}

	// By now, we know that initialization was successful, so
	// respond to the initialization request with the interface itself
	// Note: this response gets picked up by the blob manager
	InitializeBlobWorkerReply rep;
	rep.interf = bwInterf;
	recruitReply.send(rep);

	self->addActor.send(waitFailureServer(bwInterf.waitFailure.getFuture()));
	self->addActor.send(runGRVChecks(self));
	state Future<Void> selfRemoved = monitorRemoval(self);

	TraceEvent("BlobWorkerInit", self->id).log();

	try {
		loop choose {
			when(BlobGranuleFileRequest req = waitNext(bwInterf.blobGranuleFileRequest.getFuture())) {
				++self->stats.readRequests;
				++self->stats.activeReadRequests;
				self->addActor.send(handleBlobGranuleFileRequest(self, req));
			}
			when(state GranuleStatusStreamRequest req = waitNext(bwInterf.granuleStatusStreamRequest.getFuture())) {
				if (self->managerEpochOk(req.managerEpoch)) {
					if (BW_DEBUG) {
						fmt::print("Worker {0} got new granule status endpoint {1} from BM {2}\n",
						           self->id.toString(),
						           req.reply.getEndpoint().token.toString().c_str(),
						           req.managerEpoch);
					}

					// send an error to the old stream before closing it, so it doesn't get broken_promise and mark this
					// endpoint as failed
					self->currentManagerStatusStream.get().sendError(connection_failed());

					// hold a copy of the previous stream if it exists, so any waiting send calls don't get
					// proken_promise before onChange
					ReplyPromiseStream<GranuleStatusReply> copy;
					if (self->statusStreamInitialized) {
						copy = self->currentManagerStatusStream.get();
					}
					// TODO: pick a reasonable byte limit instead of just piggy-backing
					req.reply.setByteLimit(SERVER_KNOBS->BLOBWORKERSTATUSSTREAM_LIMIT_BYTES);
					self->statusStreamInitialized = true;

					self->currentManagerStatusStream.set(req.reply);
				} else {
					req.reply.sendError(blob_manager_replaced());
				}
			}
			when(AssignBlobRangeRequest _req = waitNext(bwInterf.assignBlobRangeRequest.getFuture())) {
				++self->stats.rangeAssignmentRequests;
				state AssignBlobRangeRequest assignReq = _req;
				if (BW_DEBUG) {
					fmt::print("Worker {0} assigned range [{1} - {2}) @ ({3}, {4}):\n  type={5}\n",
					           self->id.toString(),
					           assignReq.keyRange.begin.printable(),
					           assignReq.keyRange.end.printable(),
					           assignReq.managerEpoch,
					           assignReq.managerSeqno,
					           assignReq.type);
				}

				if (self->managerEpochOk(assignReq.managerEpoch)) {
					self->addActor.send(handleRangeAssign(self, assignReq, false));
				} else {
					assignReq.reply.sendError(blob_manager_replaced());
				}
			}
			when(RevokeBlobRangeRequest _req = waitNext(bwInterf.revokeBlobRangeRequest.getFuture())) {
				state RevokeBlobRangeRequest revokeReq = _req;
				if (BW_DEBUG) {
					fmt::print("Worker {0} revoked range [{1} - {2}) @ ({3}, {4}):\n  dispose={5}\n",
					           self->id.toString(),
					           revokeReq.keyRange.begin.printable(),
					           revokeReq.keyRange.end.printable(),
					           revokeReq.managerEpoch,
					           revokeReq.managerSeqno,
					           revokeReq.dispose ? "T" : "F");
				}

				if (self->managerEpochOk(revokeReq.managerEpoch)) {
					self->addActor.send(handleRangeRevoke(self, revokeReq));
				} else {
					revokeReq.reply.sendError(blob_manager_replaced());
				}
			}
			when(AssignBlobRangeRequest granuleToReassign = waitNext(self->granuleUpdateErrors.getFuture())) {
				self->addActor.send(handleRangeAssign(self, granuleToReassign, true));
			}
			when(GetGranuleAssignmentsRequest req = waitNext(bwInterf.granuleAssignmentsRequest.getFuture())) {
				if (self->managerEpochOk(req.managerEpoch)) {
					if (BW_DEBUG) {
						fmt::print("Worker {0} got granule assignments request from BM {1}\n",
						           self->id.toString(),
						           req.managerEpoch);
					}
					handleGetGranuleAssignmentsRequest(self, req);
				} else {
					req.reply.sendError(blob_manager_replaced());
				}
			}
			when(HaltBlobWorkerRequest req = waitNext(bwInterf.haltBlobWorker.getFuture())) {
				if (self->managerEpochOk(req.managerEpoch)) {
					TraceEvent("BlobWorkerHalted", self->id)
					    .detail("ReqID", req.requesterID)
					    .detail("ManagerEpoch", req.managerEpoch);
					if (BW_DEBUG) {
						fmt::print("BW {0} was halted by manager {1}\n", bwInterf.id().toString(), req.managerEpoch);
					}
					req.reply.send(Void());
					break;
				} else {
					req.reply.sendError(blob_manager_replaced());
				}
			}
			when(wait(collection)) {
				TraceEvent("BlobWorkerActorCollectionError", self->id);
				ASSERT(false);
				throw internal_error();
			}
			when(wait(selfRemoved)) {
				if (BW_DEBUG) {
					printf("Blob worker detected removal. Exiting...\n");
				}
				TraceEvent("BlobWorkerRemoved", self->id);
				break;
			}
			when(wait(self->fatalError.getFuture())) {
				TraceEvent(SevError, "BlobWorkerActorCollectionFatalErrorNotError", self->id);
				ASSERT(false);
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			self->granuleMetadata.clear();
			throw;
		}
		if (BW_DEBUG) {
			printf("Blob worker got error %s. Exiting...\n", e.name());
		}
		TraceEvent("BlobWorkerDied", self->id).errorUnsuppressed(e);
	}

	wait(self->granuleMetadata.clearAsync());
	return Void();
}

// TODO add unit tests for assign/revoke range, especially version ordering
