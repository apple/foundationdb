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

#include <tuple>
#include <utility>
#include <vector>

#include "contrib/fmt-8.0.1/include/fmt/format.h"
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
#include "flow/actorcompiler.h" // has to be last include

#define BW_DEBUG true
#define BW_REQUEST_DEBUG false

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

// TODO add global byte limit for pending and buffered deltas
struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	KeyRange keyRange;

	GranuleFiles files;
	Standalone<GranuleDeltas> currentDeltas; // only contain deltas in pendingDeltaVersion + 1, bufferedDeltaVersion

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

	// TODO FOR DEBUGGING, REMOVE
	Version waitForVersionReturned = invalidVersion;

	void resume() {
		if (resumeSnapshot.canBeSet()) {
			resumeSnapshot.send(Void());
		}
	}
};

// TODO: rename this struct
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
	}

	GranuleRangeMetadata() : lastEpoch(0), lastSeqno(0) {}
	GranuleRangeMetadata(int64_t epoch, int64_t seqno, Reference<GranuleMetadata> activeMetadata)
	  : lastEpoch(epoch), lastSeqno(seqno), activeMetadata(activeMetadata) {}

	// TODO REMOVE
	// ~GranuleRangeMetadata() { printf("Destroying granule metadata\n"); }
};

// represents a previous version of a granule, and optionally the files that compose it
struct GranuleHistoryEntry : NonCopyable, ReferenceCounted<GranuleHistoryEntry> {
	KeyRange range;
	UID granuleID;
	Version startVersion; // version of the first snapshot
	Version endVersion; // version of the last delta file

	// load files lazily, and allows for clearing old cold-queried files to save memory
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

	int changeFeedStreamReplyBufferSize = SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES / 2;

	BlobWorkerData(UID id, Database db) : id(id), db(db), stats(id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL) {}
	~BlobWorkerData() {
		if (BW_DEBUG) {
			printf("Destroying BW %s data\n", id.toString().c_str());
		}
	}

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
			}

			return true;
		}
	}
};

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
	/*
	printf(
	    "Checking granule lock: \n  mine: (%lld, %lld)\n  owner: (%lld, %lld)\n", epoch, seqno, ownerEpoch,
	ownerSeqno);
	    */
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
			wait(readGranuleFiles(&tr, &startKey, range.end, &files, granuleID, BW_DEBUG));
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
	wait(readGranuleFiles(tr, &startKey, range.end, &files, granuleID, BW_DEBUG));
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

	RangeResult totalState = wait(tr->getRange(currentRange, 100));
	// TODO is this explicit conflit range necessary with the above read?
	tr->addWriteConflictRange(currentRange);
	ASSERT(!totalState.more);

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
			// wait(updateChangeFeed(tr, KeyRef(parentGranuleID.toString()), ChangeFeedStatus::CHANGE_FEED_DESTROY));
			Key oldGranuleLockKey = blobGranuleLockKeyFor(parentGranuleRange);
			tr->clear(singleKeyRange(oldGranuleLockKey));
			tr->clear(currentRange);
			tr->clear(blobGranuleSplitBoundaryKeyRangeFor(parentGranuleID));
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

				wait(updateChangeFeed(tr, KeyRef(parentGranuleID.toString()), ChangeFeedStatus::CHANGE_FEED_STOP));
			}
		}
	} else if (BW_DEBUG) {
		fmt::print("Ignoring granule {0} split state from {1} {2} -> {3}\n",
		           currentGranuleID.toString(),
		           parentGranuleID.toString(),
		           currentState,
		           newState);
	}

	return Void();
}

// returns the split state for a given granule on granule reassignment. Assumes granule is in fact splitting, by the
// presence of the previous granule's lock key
ACTOR Future<std::pair<BlobGranuleSplitState, Version>> getGranuleSplitState(Transaction* tr,
                                                                             UID parentGranuleID,
                                                                             UID currentGranuleID) {
	Key myStateKey = blobGranuleSplitKeyFor(parentGranuleID, currentGranuleID);

	Optional<Value> st = wait(tr->get(myStateKey));
	ASSERT(st.present());
	return decodeBlobGranuleSplitValue(st.get());
}

// writeDelta file writes speculatively in the common case to optimize throughput. It creates the s3 object even though
// the data in it may not yet be committed, and even though previous delta fiels with lower versioned data may still be
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
		// TODO fix file leak here on error pre-transaction.
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

				Key dfKey = blobGranuleFileKeyFor(granuleID, 'D', currentDeltaVersion);
				Value dfValue = blobGranuleFileValueFor(fname, 0, serializedSize);
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
				return BlobFileIndex(currentDeltaVersion, fname, 0, serializedSize);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		// If this actor was cancelled, doesn't own the granule anymore, or got some other error before trying to
		// commit a transaction, we can and want to safely delete the file we wrote. Otherwise, we may have updated FDB
		// with file and cannot safely delete it.
		if (numIterations > 0) {
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("deleting delta file {0} after error {1}\n", fname, e.name());
		}
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

	// TODO REMOVE sanity checks!
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

	state Value serialized = ObjectWriter::toValue(snapshot, Unversioned());
	state size_t serializedSize = serialized.size();

	// free snapshot to reduce memory
	snapshot = Standalone<GranuleSnapshot>();

	// write to blob using multi part upload
	state Reference<IBackupFile> objectFile = wait(bwData->bstore->writeFile(fname));

	++bwData->stats.s3PutReqs;
	++bwData->stats.snapshotFilesWritten;
	bwData->stats.snapshotBytesWritten += serializedSize;

	// TODO: inject write error
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
				Key snapshotFileKey = blobGranuleFileKeyFor(granuleID, 'S', version);
				Key snapshotFileValue = blobGranuleFileValueFor(fname, 0, serializedSize);
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
			throw e;
		}
		if (BW_DEBUG) {
			fmt::print("deleting snapshot file {0} after error {1}\n", fname, e.name());
		}
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

	return BlobFileIndex(version, fname, 0, serializedSize);
}

ACTOR Future<BlobFileIndex> dumpInitialSnapshotFromFDB(Reference<BlobWorkerData> bwData,
                                                       Reference<GranuleMetadata> metadata,
                                                       UID granuleID) {
	if (BW_DEBUG) {
		fmt::print("Dumping snapshot from FDB for [{0} - {1})\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable());
	}
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);

	loop {
		state Key beginKey = metadata->keyRange.begin;
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			state Version readVersion = wait(tr->getReadVersion());
			state PromiseStream<RangeResult> rowsStream;
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(bwData,
			                                                           metadata->keyRange,
			                                                           granuleID,
			                                                           metadata->originalEpoch,
			                                                           metadata->originalSeqno,
			                                                           readVersion,
			                                                           rowsStream,
			                                                           true);

			loop {
				// TODO: use streaming range read
				// TODO: inject read error
				// TODO knob for limit?
				int lim = BUGGIFY ? 2 : 1000;
				RangeResult res = wait(tr->getRange(KeyRangeRef(beginKey, metadata->keyRange.end), lim));
				bwData->stats.bytesReadFromFDBForInitialSnapshot += res.size();
				rowsStream.send(res);
				if (res.more) {
					beginKey = keyAfter(res.back().key);
				} else {
					rowsStream.sendError(end_of_stream());
					break;
				}
			}
			BlobFileIndex f = wait(snapshotWriter);
			TraceEvent("BlobGranuleSnapshotFile", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .detail("Version", readVersion);
			DEBUG_KEY_RANGE("BlobWorkerFDBSnapshot", readVersion, metadata->keyRange, bwData->id);
			return f;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			if (BW_DEBUG) {
				fmt::print("Dumping snapshot from FDB for [{0} - {1}) got error {2}\n",
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable(),
				           e.name());
			}
			state Error err = e;
			wait(tr->onError(e));
			TraceEvent(SevWarn, "BlobGranuleInitialSnapshotRetry", bwData->id)
			    .detail("Granule", metadata->keyRange)
			    .error(err);
		}
	}
}

// files might not be the current set of files in metadata, in the case of doing the initial snapshot of a granule that
// was split.
// FIXME: only pass metadata->keyRange
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

	chunk.snapshotFile = BlobFilePointerRef(filenameArena, snapshotF.filename, snapshotF.offset, snapshotF.length);
	compactBytesRead += snapshotF.length;
	int deltaIdx = files.deltaFiles.size() - 1;
	while (deltaIdx >= 0 && files.deltaFiles[deltaIdx].version > snapshotVersion) {
		deltaIdx--;
	}
	deltaIdx++;
	Version lastDeltaVersion = invalidVersion;
	while (deltaIdx < files.deltaFiles.size() && files.deltaFiles[deltaIdx].version <= version) {
		BlobFileIndex deltaF = files.deltaFiles[deltaIdx];
		chunk.deltaFiles.emplace_back_deep(filenameArena, deltaF.filename, deltaF.offset, deltaF.length);
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

		/*printf("  SnapshotFile:\n    %s\n", chunk.snapshotFile.get().toString().c_str());
		printf("  DeltaFiles:\n");
		for (auto& df : chunk.deltaFiles) {
		    printf("    %s\n", df.toString().c_str());
		}*/
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
			    wait(readBlobGranule(chunk, metadata->keyRange, version, bwData->bstore, &bwData->stats));

			// TODO: inject read error

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
	wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));

	if (BW_DEBUG) {
		fmt::print("Granule [{0} - {1}) checking with BM for re-snapshot after {2} bytes\n",
		           metadata->keyRange.begin.printable(),
		           metadata->keyRange.end.printable(),
		           metadata->bytesInNewDeltaFiles);
	}

	TraceEvent("BlobGranuleSnapshotCheck", bwData->id)
	    .detail("Granule", metadata->keyRange)
	    .detail("Version", reSnapshotVersion);

	// Save these from the start so repeated requests are idempotent
	// Need to retry in case response is dropped or manager changes. Eventually, a manager will
	// either reassign the range with continue=true, or will revoke the range. But, we will keep the
	// range open at this version for reads until that assignment change happens
	metadata->resumeSnapshot.reset();
	state int64_t statusEpoch = metadata->continueEpoch;
	state int64_t statusSeqno = metadata->continueSeqno;
	// TODO its own knob or something better? This is wrong in case of rollbacks
	state bool writeHot = versionsSinceLastSnapshot <= SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS;
	loop {
		loop {
			try {
				wait(bwData->currentManagerStatusStream.get().onReady());
				bwData->currentManagerStatusStream.get().send(GranuleStatusReply(metadata->keyRange,
				                                                                 true,
				                                                                 writeHot,
				                                                                 statusEpoch,
				                                                                 statusSeqno,
				                                                                 granuleID,
				                                                                 metadata->initialSnapshotVersion,
				                                                                 reSnapshotVersion));
				break;
			} catch (Error& e) {
				wait(bwData->currentManagerStatusStream.onChange());
			}
		}

		// TODO: figure out why the status stream on change isn't working
		// We could just do something like statusEpoch, save down the original status stream
		// and compare it to the current one
		if (statusEpoch < bwData->currentManagerEpoch) {
			break;
		}

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
	TraceEvent("BlobGranuleSnapshotFile", bwData->id)
	    .detail("Granule", metadata->keyRange)
	    .detail("Version", metadata->durableDeltaVersion.get());
	// TODO: this could read from FDB instead if it knew there was a large range clear at the end or
	// it knew the granule was small, or something

	// wait for file updater to make sure that last delta file is in the metadata before
	while (metadata->files.deltaFiles.empty() || metadata->files.deltaFiles.back().version < reSnapshotVersion) {
		wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
	}
	BlobFileIndex reSnapshotIdx =
	    wait(compactFromBlob(bwData, metadata, granuleID, metadata->files, reSnapshotVersion));
	return reSnapshotIdx;
}

ACTOR Future<Void> handleCompletedDeltaFile(Reference<BlobWorkerData> bwData,
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
			fmt::print("Popping change feed {0} at {1}\n", cfKey.printable(), completedDeltaFile.version);
		}
		// FIXME: for a write-hot shard, we could potentially batch these and only pop the largest one after several
		// have completed
		// FIXME: also have these be async, have each pop change feed wait on the prior one, wait on them before
		// re-snapshotting
		Future<Void> popFuture = bwData->db->popChangeFeedMutations(cfKey, completedDeltaFile.version);
		wait(popFuture);
	}
	while (!rollbacksCompleted->empty() && completedDeltaFile.version >= rollbacksCompleted->front().second) {
		fmt::print("Completed rollback {0} -> {1} with delta file {2}\n",
		           rollbacksCompleted->front().second,
		           rollbacksCompleted->front().first,
		           completedDeltaFile.version);
		rollbacksCompleted->pop_front();
	}
	return Void();
}

// if we get an i/o error updating files, or a rollback, reassign the granule to ourselves and start fresh
// FIXME: is this the correct set of errors?
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

// TODO REMOVE once correctness clean
#define DEBUG_BW_START_VERSION invalidVersion
#define DEBUG_BW_END_VERSION invalidVersion
#define DEBUG_BW_WAIT_VERSION invalidVersion
#define DEBUG_BW_VERSION(v) DEBUG_BW_START_VERSION <= v&& v <= DEBUG_BW_END_VERSION

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

#define DEBUG_WAIT_VERSION_COMMITTED false
ACTOR Future<Void> waitOnCFVersion(Reference<GranuleMetadata> metadata,
                                   Version original /*TODO REMOVE, just for debugging*/,
                                   Version waitVersion) {
	if (DEBUG_BW_VERSION(original) && DEBUG_WAIT_VERSION_COMMITTED) {
		fmt::print("WVC {0}:   waiting for {1} \n", original, waitVersion);
	}
	loop {
		try {
			if (DEBUG_BW_VERSION(original) && DEBUG_WAIT_VERSION_COMMITTED) {
				if (metadata->activeCFData.get().isValid()) {
					fmt::print(
					    "WVC {0}:     WAL (currently {1})\n", original, metadata->activeCFData.get()->getVersion());
				} else {
					fmt::print("WVC {0}:     invalid\n", original, metadata->activeCFData.get()->getVersion());
				}
			}
			// if not valid, we're about to be cancelled anyway
			state Future<Void> atLeast = metadata->activeCFData.get().isValid()
			                                 ? metadata->activeCFData.get()->whenAtLeast(waitVersion)
			                                 : Never();
			choose {
				when(wait(atLeast)) {
					if (DEBUG_BW_VERSION(original) && DEBUG_WAIT_VERSION_COMMITTED) {
						fmt::print("WVC {0}:   got at least {1} \n", original, waitVersion);
					}
					break;
				}
				when(wait(metadata->activeCFData.onChange())) {
					if (DEBUG_BW_VERSION(original) && DEBUG_WAIT_VERSION_COMMITTED) {
						fmt::print("WVC {0}:     cfOnChange \n", original);
					}
				}
			}
		} catch (Error& e) {
			if (DEBUG_BW_VERSION(original) && DEBUG_WAIT_VERSION_COMMITTED) {
				fmt::print("WVC {0}:   got error {1} \n", original, e.name());
			}
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}

			// if waiting on a parent granule change feed and we change to the child, the parent will get end_of_stream,
			// which could cause this waiting whenAtLeast to get change_feed_cancelled. We should simply retry and wait
			// a bit, as blobGranuleUpdateFiles will switch to the new change feed
			if (DEBUG_BW_VERSION(original) && DEBUG_WAIT_VERSION_COMMITTED) {
				if (BW_DEBUG) {
					fmt::print("WVC {0}:   unexpected error {1}\n", original, e.name());
				}
				throw e;
			}
			wait(delay(0.05));
		}
	}

	if (DEBUG_BW_VERSION(original) && DEBUG_WAIT_VERSION_COMMITTED) {
		fmt::print("WVC {0}:   got \n", original);
	}

	// sanity check to make sure whenAtLeast didn't return early
	if (waitVersion > metadata->waitForVersionReturned) {
		metadata->waitForVersionReturned = waitVersion;
	}

	// stop after change feed callback
	wait(delay(0, TaskPriority::BlobWorkerReadChangeFeed));

	return Void();
}

ACTOR Future<Void> waitCommittedGrv(Reference<BlobWorkerData> bwData,
                                    Reference<GranuleMetadata> metadata,
                                    Version version) {
	if (DEBUG_BW_VERSION(version) && DEBUG_WAIT_VERSION_COMMITTED) {
		fmt::print("WVC {0}:   grv start\n", version);
	}
	// TODO REMOVE debugs
	if (version > bwData->grvVersion.get()) {
		if (DEBUG_BW_VERSION(version) && DEBUG_WAIT_VERSION_COMMITTED) {
			fmt::print("WVC {0}:     getting grv\n", version);
		}
		/*if (BW_DEBUG) {
		    fmt::print("waitVersionCommitted waiting {0}\n", version);
		}*/
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
	if (DEBUG_BW_VERSION(version) && DEBUG_WAIT_VERSION_COMMITTED) {
		fmt::print("WVC {0}:     got grv\n", version);
	}
	wait(waitOnCFVersion(metadata, version, grvVersion));
	return Void();
}

ACTOR Future<Void> waitVersionCommitted(Reference<BlobWorkerData> bwData,
                                        Reference<GranuleMetadata> metadata,
                                        Version version) {
	// If GRV is way in the future, we know we can't roll back more than 5 seconds (or whatever this knob is set to)
	// worth of versions
	if (DEBUG_BW_VERSION(version) && DEBUG_WAIT_VERSION_COMMITTED) {
		fmt::print("WVC {0}: starting\n", version);
	}
	wait(waitCommittedGrv(bwData, metadata, version) ||
	     waitOnCFVersion(metadata, version, version + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS));
	if (DEBUG_BW_VERSION(version) && DEBUG_WAIT_VERSION_COMMITTED) {
		fmt::print("WVC {0}: done\n", version);
	}
	if (version > metadata->knownCommittedVersion) {
		metadata->knownCommittedVersion = version;
	}
	return Void();
}

// updater for a single granule
// TODO: this is getting kind of large. Should try to split out this actor if it continues to grow?
// FIXME: handle errors here (forward errors)
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

		cfKey = StringRef(startState.granuleID.toString());
		if (startState.parentGranule.present()) {
			oldCFKey = StringRef(startState.parentGranule.get().second.toString());
		}

		if (BW_DEBUG) {
			fmt::print("Granule File Updater Starting for [{0} - {1}) @ ({2}, {3}):\n",
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           metadata->originalEpoch,
			           metadata->originalSeqno);
			fmt::print("  CFID: {}\n", startState.granuleID.toString());
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
				BlobFileIndex fromFDB = wait(dumpInitialSnapshotFromFDB(bwData, metadata, startState.granuleID));
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

		Reference<ChangeFeedData> newCFData = makeReference<ChangeFeedData>();

		if (startState.parentGranule.present() && startVersion < startState.changeFeedStartVersion) {
			// read from parent change feed up until our new change feed is started
			// Required to have canReadPopped = false, otherwise another granule can take over the change feed, and pop
			// it. That could cause this worker to think it has the full correct set of data if it then reads the data,
			// until it checks the granule lock again.
			// passing false for canReadPopped means we will get an exception if we try to read any popped data, killing
			// this actor
			readOldChangeFeed = true;

			oldChangeFeedFuture = bwData->db->getChangeFeedStream(newCFData,
			                                                      oldCFKey.get(),
			                                                      startVersion + 1,
			                                                      startState.changeFeedStartVersion,
			                                                      metadata->keyRange,
			                                                      bwData->changeFeedStreamReplyBufferSize,
			                                                      false);

		} else {
			readOldChangeFeed = false;
			changeFeedFuture = bwData->db->getChangeFeedStream(newCFData,
			                                                   cfKey,
			                                                   startVersion + 1,
			                                                   MAX_VERSION,
			                                                   metadata->keyRange,
			                                                   bwData->changeFeedStreamReplyBufferSize,
			                                                   false);
		}

		// Start actors BEFORE setting new change feed data to ensure the change feed data is properly initialized by
		// the client
		metadata->activeCFData.set(newCFData);

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
						wait(handleCompletedDeltaFile(bwData,
						                              metadata,
						                              completedFile,
						                              cfKey,
						                              startState.changeFeedStartVersion,
						                              &rollbacksCompleted));
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
				/*if (DEBUG_BW_VERSION(metadata->bufferedDeltaVersion)) {
				    fmt::print("BW waiting mutations after ({0})\n", metadata->bufferedDeltaVersion);
				}*/
				// Even if there are no new mutations, there still might be readers waiting on durableDeltaVersion
				// to advance. We need to check whether any outstanding files have finished so we don't wait on
				// mutations forever
				choose {
					when(Standalone<VectorRef<MutationsAndVersionRef>> _mutations =
					         waitNext(metadata->activeCFData.get()->mutations.getFuture())) {
						/*if (DEBUG_BW_VERSION(metadata->bufferedDeltaVersion)) {
						    fmt::print("BW got mutations after ({0}): {1} - {2} ({3})\n",
						               metadata->bufferedDeltaVersion,
						               _mutations.front().version,
						               _mutations.back().version,
						               _mutations.size());
						}*/
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

						// if we just got mutations, we haven't buffered them yet, so waitForVersion can't have returned
						// this version yet
						if (mutations.front().version <= metadata->waitForVersionReturned) {
							fmt::print("ERROR: WaitForVersion returned early for granule [{0} - {1}). "
							           "waitForVersionReturned={2}, mutationVersion={3} !!!\n",
							           metadata->keyRange.begin.printable(),
							           metadata->keyRange.end.printable(),
							           metadata->waitForVersionReturned,
							           mutations.front().version);
						}
						ASSERT(mutations.front().version > metadata->waitForVersionReturned);
					}
					when(wait(inFlightFiles.empty() ? Never() : success(inFlightFiles.front().future))) {
						//  TODO REMOVE
						/*if (DEBUG_BW_VERSION(metadata->bufferedDeltaVersion)) {
						    fmt::print("BW got file before waiting for mutations after {0}\n",
						               metadata->bufferedDeltaVersion);
						}*/
					}
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

				Reference<ChangeFeedData> newCFData = makeReference<ChangeFeedData>();

				changeFeedFuture = bwData->db->getChangeFeedStream(newCFData,
				                                                   cfKey,
				                                                   startState.changeFeedStartVersion,
				                                                   MAX_VERSION,
				                                                   metadata->keyRange,
				                                                   bwData->changeFeedStreamReplyBufferSize,
				                                                   false);

				// Start actors BEFORE setting new change feed data to ensure the change feed data is properly
				// initialized by the client
				metadata->activeCFData.set(newCFData);
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
								// TODO REMOVE, for debugging
								if (rollbacksInProgress.front().first != rollbackVersion) {
									fmt::print("Found out of order rollbacks! Current in progress: {0}, mutation "
									           "version: {1}\n",
									           rollbacksInProgress.front().first,
									           rollbackVersion);
								}
								ASSERT(rollbacksInProgress.front().first == rollbackVersion);
								if (rollbacksInProgress.front().first != rollbackVersion) {
									fmt::print("Found out of order rollbacks! Current in progress: {0}, rollback "
									           "version: {1}\n",
									           rollbacksInProgress.front().second,
									           deltas.version);
								}
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

									if (BW_DEBUG) {
										fmt::print("BW skipping rollback {0} -> {1} completely\n",
										           deltas.version,
										           rollbackVersion);
									}
									// Still have to add to rollbacksCompleted. If we later roll the granule back past
									// this because of cancelling a delta file, we need to count this as in progress so
									// we can match the rollback mutation to a rollbackInProgress when we restart the
									// stream.
									rollbacksCompleted.push_back(std::pair(rollbackVersion, deltas.version));
								} else {
									if (BW_DEBUG) {
										fmt::print("BW [{0} - {1}) ROLLBACK @ {2} -> {3}\n",
										           metadata->keyRange.begin.printable(),
										           metadata->keyRange.end.printable(),
										           deltas.version,
										           rollbackVersion);
										TraceEvent(SevWarn, "GranuleRollback", bwData->id)
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

									// Reset change feeds to cfRollbackVersion
									if (cfRollbackVersion < metadata->waitForVersionReturned) {
										fmt::print("Rollback resetting waitForVersionReturned {0} -> {1}\n",
										           metadata->waitForVersionReturned,
										           cfRollbackVersion);
										metadata->waitForVersionReturned = cfRollbackVersion;
									}

									Reference<ChangeFeedData> newCFData = makeReference<ChangeFeedData>();

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
										    bwData->db->getChangeFeedStream(newCFData,
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
										    bwData->db->getChangeFeedStream(newCFData,
										                                    cfKey,
										                                    cfRollbackVersion + 1,
										                                    MAX_VERSION,
										                                    metadata->keyRange,
										                                    bwData->changeFeedStreamReplyBufferSize,
										                                    false);
									}

									// Start actors BEFORE setting new change feed data to ensure the change feed data
									// is properly initialized by the client
									metadata->activeCFData.set(newCFData);

									justDidRollback = true;
									break;
								}
							}
						} else if (!rollbacksInProgress.empty() && rollbacksInProgress.front().first < deltas.version &&
						           rollbacksInProgress.front().second > deltas.version) {
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
								    .detail("ChangeFeedID", readOldChangeFeed ? oldCFKey.get() : cfKey)
								    .detail("OldChangeFeed", readOldChangeFeed ? "T" : "F");
							}
							if (DEBUG_BW_VERSION(deltas.version)) {
								fmt::print("BWB {0}: ({1})\n", deltas.version, deltas.mutations.size());
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
					TraceEvent("BlobGranuleDeltaFile", bwData->id)
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
				    !readOldChangeFeed) {
					if (BW_DEBUG && !inFlightFiles.empty()) {
						fmt::print("Granule [{0} - {1}) ready to re-snapshot after {2} > {3} bytes, waiting for "
						           "outstanding {4} files to finish\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
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
								wait(handleCompletedDeltaFile(bwData,
								                              metadata,
								                              completedFile,
								                              cfKey,
								                              startState.changeFeedStartVersion,
								                              &rollbacksCompleted));
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
		// TODO REMOVE
		if (BW_DEBUG) {
			fmt::print("BGUF {0} [{1} - {2}) got error {3}\n",
			           startState.granuleID.toString(),
			           metadata->keyRange.begin.printable(),
			           metadata->keyRange.end.printable(),
			           e.name());
		}
		// Free last change feed data
		metadata->activeCFData.set(Reference<ChangeFeedData>());

		if (e.code() == error_code_operation_cancelled) {
			throw;
		}

		if (metadata->cancelled.canBeSet()) {
			metadata->cancelled.send(Void());
		}

		if (e.code() == error_code_granule_assignment_conflict) {
			TraceEvent(SevInfo, "GranuleAssignmentConflict", bwData->id).detail("Granule", metadata->keyRange);
			return Void();
		} else {
			++bwData->stats.granuleUpdateErrors;
			if (BW_DEBUG) {
				fmt::print("Granule file updater for [{0} - {1}) got error {2}, exiting\n",
				           metadata->keyRange.begin.printable(),
				           metadata->keyRange.end.printable(),
				           e.name());
			}
			TraceEvent(SevWarn, "GranuleFileUpdaterError", bwData->id).detail("Granule", metadata->keyRange).error(e);

			if (granuleCanRetry(e)) {
				// explicitly cancel all outstanding write futures BEFORE updating promise stream, to ensure they
				// can't update files after the re-assigned granule acquires the lock
				// do it backwards though because future depends on previous one, so it could cause a cascade
				for (int i = inFlightFiles.size() - 1; i >= 0; i--) {
					inFlightFiles[i].future.cancel();
				}

				// if we retry and re-open, we need to use a normal request (no continue or reassign) and update the
				// seqno
				metadata->originalReq.managerEpoch = metadata->continueEpoch;
				metadata->originalReq.managerSeqno = metadata->continueSeqno;
				metadata->originalReq.type = AssignRequestType::Normal;

				bwData->granuleUpdateErrors.send(metadata->originalReq);
			}
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

			// while the start version of the current granule's parent not past the last known start version,
			// walk backwards
			while (curHistory.value.parentGranules.size() > 0 &&
			       curHistory.value.parentGranules[0].second >= stopVersion) {
				state GranuleHistory next;
				loop {
					try {
						Optional<Value> v = wait(tr.get(blobGranuleHistoryKeyFor(
						    curHistory.value.parentGranules[0].first, curHistory.value.parentGranules[0].second)));
						ASSERT(v.present());
						next = GranuleHistory(curHistory.value.parentGranules[0].first,
						                      curHistory.value.parentGranules[0].second,
						                      decodeBlobGranuleHistoryValue(v.get()));
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}

				ASSERT(next.version != invalidVersion);
				// granule next.granuleID goes from the version range [next.version, curHistory.version]
				historyEntryStack.push_back(makeReference<GranuleHistoryEntry>(
				    next.range, next.value.granuleID, next.version, curHistory.version));
				curHistory = next;
			}

			if (!historyEntryStack.empty()) {
				Version oldestStartVersion = historyEntryStack.back()->startVersion;
				// TODO REMOVE eventually, for debugging
				if (stopVersion != oldestStartVersion && stopVersion != invalidVersion) {
					fmt::print("Finished, stopVersion={0}, curHistory.version={1}\n", stopVersion, oldestStartVersion);
				}
				ASSERT(stopVersion == oldestStartVersion || stopVersion == invalidVersion);
			} else {
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
				i--;
			}
			int skipped = historyEntryStack.size() - 1 - i;

			while (i >= 0) {
				auto prevRanges = bwData->granuleHistory.rangeContaining(historyEntryStack[i]->range.begin);

				// sanity check
				ASSERT(!prevRanges.value().isValid() ||
				       prevRanges.value()->endVersion == historyEntryStack[i]->startVersion);

				historyEntryStack[i]->parentGranule = prevRanges.value();
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
		if (BW_DEBUG) {
			fmt::print("Loading blob granule history got unexpected error {}\n", e.name());
		}
		// TODO this should never happen?
		ASSERT(false);
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

	ASSERT(metadata->activeCFData.get().isValid());

	if (v == DEBUG_BW_WAIT_VERSION) {
		fmt::print("{0}) [{1} - {2}) waiting for {3}\n  readable:{4}\n  bufferedDelta={5}\n  pendingDelta={6}\n  "
		           "durableDelta={7}\n  pendingSnapshot={8}\n  durableSnapshot={9}\n",
		           v,
		           metadata->keyRange.begin.printable().c_str(),
		           metadata->keyRange.end.printable().c_str(),
		           v,
		           metadata->readable.isSet() ? "T" : "F",
		           metadata->activeCFData.get()->getVersion(),
		           metadata->pendingDeltaVersion,
		           metadata->durableDeltaVersion.get(),
		           metadata->pendingSnapshotVersion,
		           metadata->durableSnapshotVersion.get());
	}

	if (v <= metadata->activeCFData.get()->getVersion() &&
	    (v <= metadata->durableDeltaVersion.get() ||
	     metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion) &&
	    (v <= metadata->durableSnapshotVersion.get() ||
	     metadata->durableSnapshotVersion.get() == metadata->pendingSnapshotVersion)) {
		// TODO REMOVE debugging
		if (v > metadata->waitForVersionReturned) {
			metadata->waitForVersionReturned = v;
		}
		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) already done\n", v);
		}
		return Void();
	}

	// wait for change feed version to catch up to ensure we have all data
	if (metadata->activeCFData.get()->getVersion() < v) {
		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) waiting for CF version (currently {1})\n", v, metadata->activeCFData.get()->getVersion());
		}

		wait(metadata->activeCFData.get()->whenAtLeast(v));
		ASSERT(metadata->activeCFData.get()->getVersion() >= v);

		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) got CF version {1}\n", v, metadata->activeCFData.get()->getVersion());
		}
		// TODO REMOVE debugging
		if (v > metadata->waitForVersionReturned) {
			metadata->waitForVersionReturned = v;
		}
	}

	// wait for any pending delta and snapshot files as of the moment the change feed version caught up.
	state Version pendingDeltaV = metadata->pendingDeltaVersion;
	state Version pendingSnapshotV = metadata->pendingSnapshotVersion;

	// If there are mutations that are no longer buffered but have not been
	// persisted to a delta file that are necessary for the query, wait for them
	if (pendingDeltaV > metadata->durableDeltaVersion.get() && v > metadata->durableDeltaVersion.get()) {
		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) waiting for DDV {1} < {2}\n", v, metadata->durableDeltaVersion.get(), pendingDeltaV);
		}

		wait(metadata->durableDeltaVersion.whenAtLeast(pendingDeltaV));
		ASSERT(metadata->durableDeltaVersion.get() >= pendingDeltaV);

		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) got DDV {1} >= {2}\n", v, metadata->durableDeltaVersion.get(), pendingDeltaV);
		}
	}

	// This isn't strictly needed, but if we're in the process of re-snapshotting, we'd likely rather
	// return that snapshot file than the previous snapshot file and all its delta files.
	if (pendingSnapshotV > metadata->durableSnapshotVersion.get() && v > metadata->durableSnapshotVersion.get()) {
		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) waiting for DSV {1} < {2}\n", v, metadata->durableSnapshotVersion.get(), pendingSnapshotV);
		}

		wait(metadata->durableSnapshotVersion.whenAtLeast(pendingSnapshotV));
		ASSERT(metadata->durableSnapshotVersion.get() >= pendingSnapshotV);

		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) got DSV {1} >= {2}\n", v, metadata->durableSnapshotVersion.get(), pendingSnapshotV);
		}
	}

	// There is a race here - we wait for pending delta files before this to finish, but while we do, we
	// kick off another delta file and roll the mutations. In that case, we must return the new delta
	// file instead of in memory mutations, so we wait for that delta file to complete

	if (metadata->pendingDeltaVersion >= v) {
		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) waiting for DDV again {1} < {2}\n", v, metadata->durableDeltaVersion.get(), v);
		}

		wait(metadata->durableDeltaVersion.whenAtLeast(v));
		ASSERT(metadata->durableDeltaVersion.get() >= v);

		if (v == DEBUG_BW_WAIT_VERSION) {
			fmt::print("{0}) got DDV again {1} >= {2}\n", v, metadata->durableDeltaVersion.get(), v);
		}
	}

	if (v == DEBUG_BW_WAIT_VERSION) {
		fmt::print("{0}) done\n", v);
	}

	return Void();
}

ACTOR Future<Void> handleBlobGranuleFileRequest(Reference<BlobWorkerData> bwData, BlobGranuleFileRequest req) {
	if (BW_REQUEST_DEBUG || DEBUG_BW_WAIT_VERSION == req.readVersion) {
		fmt::print("BW {0} processing blobGranuleFileRequest for range [{1} - {2}) @ {3}\n",
		           bwData->id.toString(),
		           req.keyRange.begin.printable(),
		           req.keyRange.end.printable(),
		           req.readVersion);
	}

	try {
		// TODO REMOVE in api V2
		ASSERT(req.beginVersion == 0);
		state BlobGranuleFileReply rep;
		state std::vector<Reference<GranuleMetadata>> granules;

		auto checkRanges = bwData->granuleMetadata.intersectingRanges(req.keyRange);
		// check for gaps as errors and copy references to granule metadata before yielding or doing any
		// work
		KeyRef lastRangeEnd = req.keyRange.begin;

		for (auto& r : checkRanges) {
			bool isValid = r.value().activeMetadata.isValid();
			if (lastRangeEnd < r.begin() || !isValid) {
				if (BW_REQUEST_DEBUG || DEBUG_BW_WAIT_VERSION == req.readVersion) {
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
			if (BW_REQUEST_DEBUG || DEBUG_BW_WAIT_VERSION == req.readVersion) {
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
					// TODO REMOVE, useful for debugging for now
					if (cur->endVersion != expectedEndVersion) {
						fmt::print("Active granule [{0} - {1}) does not have history ancestor!!. Start is {2}, "
						           "ancestor is [{3} - {4}) ({5}) V[{6} - {7}). SearchKey={8}\n",
						           metadata->keyRange.begin.printable(),
						           metadata->keyRange.end.printable(),
						           expectedEndVersion,
						           cur->range.begin.printable(),
						           cur->range.end.printable(),
						           cur->granuleID.toString(),
						           cur->startVersion,
						           cur->endVersion,
						           historySearchKey.printable());
					}
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

				if (BW_REQUEST_DEBUG || DEBUG_BW_WAIT_VERSION == req.readVersion) {
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
					cur->files = loadHistoryFiles(bwData->db, cur->granuleID, BW_DEBUG);
				}

				choose {
					when(GranuleFiles _f = wait(cur->files)) { chunkFiles = _f; }
					when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
				}

				ASSERT(!chunkFiles.snapshotFiles.empty());
				ASSERT(!chunkFiles.deltaFiles.empty());
				// TODO remove eventually, for help debugging asserts
				if (chunkFiles.deltaFiles.back().version <= req.readVersion ||
				    chunkFiles.snapshotFiles.front().version > req.readVersion) {
					fmt::print("Time Travel read version {0} out of bounds!\n  current granule initial version: {1}\n  "
					           "snapshot files ({2}):\n",
					           req.readVersion,
					           metadata->initialSnapshotVersion,
					           chunkFiles.snapshotFiles.size());
					for (auto& f : chunkFiles.snapshotFiles) {
						fmt::print("    {0}}\n", f.version);
					}
					fmt::print("  delta files {0}:\n", chunkFiles.deltaFiles.size());
					for (auto& f : chunkFiles.deltaFiles) {
						fmt::print("    {0}\n", f.version);
					}
				}
				ASSERT(chunkFiles.deltaFiles.back().version > req.readVersion);
				ASSERT(chunkFiles.snapshotFiles.front().version <= req.readVersion);
			} else {
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
						// we can get change feed cancelled from whenAtLeast. This is effectively
						if (e.code() != error_code_change_feed_cancelled) {
							throw e;
						}
						// wait 1ms and try again
						wait(delay(0.001));
					}
					if ((BW_REQUEST_DEBUG || DEBUG_BW_WAIT_VERSION == req.readVersion) &&
					    metadata->activeCFData.get().isValid()) {
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

			BlobGranuleChunkRef chunk;
			// TODO change in V2
			chunk.includedVersion = req.readVersion;
			chunk.keyRange = KeyRangeRef(StringRef(rep.arena, chunkRange.begin), StringRef(rep.arena, chunkRange.end));

			// handle snapshot files
			// TODO refactor the "find snapshot file" logic to GranuleFiles?
			// FIXME: binary search instead of linear search, especially when file count is large
			int i = chunkFiles.snapshotFiles.size() - 1;
			while (i >= 0 && chunkFiles.snapshotFiles[i].version > req.readVersion) {
				i--;
			}
			// because of granule history, we should always be able to find the desired snapshot
			// version, and have thrown blob_granule_transaction_too_old earlier if not possible.
			if (i < 0) {
				fmt::print("req @ {0} >= initial snapshot {1} but can't find snapshot in ({2}) files:\n",
				           req.readVersion,
				           metadata->initialSnapshotVersion,
				           chunkFiles.snapshotFiles.size());
				for (auto& f : chunkFiles.snapshotFiles) {
					fmt::print("  {0}", f.version);
				}
			}
			ASSERT(i >= 0);

			BlobFileIndex snapshotF = chunkFiles.snapshotFiles[i];
			chunk.snapshotFile = BlobFilePointerRef(rep.arena, snapshotF.filename, snapshotF.offset, snapshotF.length);
			Version snapshotVersion = chunkFiles.snapshotFiles[i].version;

			// handle delta files
			// cast this to an int so i going to -1 still compares properly
			int lastDeltaFileIdx = chunkFiles.deltaFiles.size() - 1;
			i = lastDeltaFileIdx;
			// skip delta files that are too new
			while (i >= 0 && chunkFiles.deltaFiles[i].version > req.readVersion) {
				i--;
			}
			if (i < lastDeltaFileIdx) {
				// we skipped one file at the end with a larger read version, this will actually contain
				// our query version, so add it back.
				i++;
			}
			// only include delta files after the snapshot file
			int j = i;
			while (j >= 0 && chunkFiles.deltaFiles[j].version > snapshotVersion) {
				j--;
			}
			j++;
			while (j <= i) {
				BlobFileIndex deltaF = chunkFiles.deltaFiles[j];
				chunk.deltaFiles.emplace_back_deep(rep.arena, deltaF.filename, deltaF.offset, deltaF.length);
				bwData->stats.readReqDeltaBytesReturned += deltaF.length;
				j++;
			}

			// new deltas (if version is larger than version of last delta file)
			// FIXME: do trivial key bounds here if key range is not fully contained in request key
			// range

			if (req.readVersion > metadata->durableDeltaVersion.get()) {
				if (metadata->durableDeltaVersion.get() != metadata->pendingDeltaVersion) {
					fmt::print("real-time read [{0} - {1}) @ {2} doesn't have mutations!! durable={3}, pending={4}\n",
					           metadata->keyRange.begin.printable(),
					           metadata->keyRange.end.printable(),
					           req.readVersion,
					           metadata->durableDeltaVersion.get(),
					           metadata->pendingDeltaVersion);
				}
				ASSERT(metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion);
				rep.arena.dependsOn(metadata->currentDeltas.arena());
				for (auto& delta : metadata->currentDeltas) {
					if (delta.version > req.readVersion) {
						break;
					}
					chunk.newDeltas.push_back_deep(rep.arena, delta);
				}
			}

			rep.chunks.push_back(rep.arena, chunk);

			bwData->stats.readReqTotalFilesReturned += chunk.deltaFiles.size() + int(chunk.snapshotFile.present());
			readThrough = chunk.keyRange.end;

			wait(yield(TaskPriority::DefaultEndpoint));
		}
		req.reply.send(rep);
		--bwData->stats.activeReadRequests;
	} catch (Error& e) {
		// fmt::print("Error in BGFRequest {0}\n", e.name());
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

ACTOR Future<GranuleStartState> openGranule(Reference<BlobWorkerData> bwData, AssignBlobRangeRequest req) {
	ASSERT(req.type != AssignRequestType::Continue);
	state Transaction tr(bwData->db);
	state Key lockKey = blobGranuleLockKeyFor(req.keyRange);

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
			state Future<Optional<GranuleHistory>> fHistory = getLatestGranuleHistory(&tr, req.keyRange);
			Optional<GranuleHistory> history = wait(fHistory);
			info.history = history;
			Optional<Value> prevLockValue = wait(fLockValue);
			state bool hasPrevOwner = prevLockValue.present();
			if (hasPrevOwner) {
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
				// FIXME: use actual 16 bytes of UID instead of converting it to 32 character string and
				// then that to bytes

				if (info.history.present()) {
					// if this granule is derived from a split or merge, this history entry is already
					// present (written by the blob manager)
					info.granuleID = info.history.get().value.granuleID;
				} else {
					// FIXME: could avoid max uid for granule ids here
					// if this granule is not derived from a split or merge, create the granule id here
					info.granuleID = deterministicRandom()->randomUniqueID();
				}
				wait(updateChangeFeed(
				    &tr, StringRef(info.granuleID.toString()), ChangeFeedStatus::CHANGE_FEED_CREATE, req.keyRange));
				info.doSnapshot = true;
				info.previousDurableVersion = invalidVersion;
			}

			tr.set(lockKey, blobGranuleLockValueFor(req.managerEpoch, req.managerSeqno, info.granuleID));
			wait(krmSetRange(&tr, blobGranuleMappingKeys.begin, req.keyRange, blobGranuleMappingValueFor(bwData->id)));

			// If anything in previousGranules, need to do the handoff logic and set
			// ret.previousChangeFeedId, and the previous durable version will come from the previous
			// granules
			if (info.history.present() && info.history.get().value.parentGranules.size() > 0) {
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
						// was already assigned, use change feed start version
						ASSERT(granuleSplitState.second > 0);
						info.changeFeedStartVersion = granuleSplitState.second;
					} else if (granuleSplitState.first == BlobGranuleSplitState::Initialized) {
						wait(updateGranuleSplitState(&tr,
						                             info.parentGranule.get().first,
						                             info.parentGranule.get().second,
						                             info.granuleID,
						                             BlobGranuleSplitState::Assigned));
						// change feed was created as part of this transaction, changeFeedStartVersion
						// will be set later
					} else {
						// this sub-granule is done splitting, no need for split logic.
						ASSERT(granuleSplitState.first == BlobGranuleSplitState::Done);
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

			TraceEvent("GranuleOpen", bwData->id).detail("Granule", req.keyRange);

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
	// since changeBlobRange is used for assigns and revokes,
	// we assert that assign type is specified iff this is an
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

		// if this granule already has it, and this was a special assignment (i.e. a new blob manager is
		// trying to reassign granules), then just continue

		// TODO this needs to also have the condition
		if (active && assignType.get() == AssignRequestType::Reassign && r.value().activeMetadata.isValid() &&
		    r.begin() == keyRange.begin && r.end() == keyRange.end) {
			r.value().lastEpoch = epoch;
			r.value().lastSeqno = seqno;
			r.value().activeMetadata->continueEpoch = epoch;
			r.value().activeMetadata->continueSeqno = seqno;
			alreadyAssigned = true;
			break;
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

ACTOR Future<Void> registerBlobWorker(Reference<BlobWorkerData> bwData, BlobWorkerInterface interf) {
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
				fmt::print("Registered blob worker {}\n", interf.id().toString());
			}
			return Void();
		} catch (Error& e) {
			if (BW_DEBUG) {
				fmt::print("Registering blob worker {0} got error {1}\n", interf.id().toString(), e.name());
			}
			wait(tr->onError(e));
		}
	}
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
				// TODO: should we just return here rather than throw and kill BW
			}
		}

		throw;
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
			// printf("GRV checker sleeping\n");
			wait(bwData->doGRVCheck.getFuture());
			bwData->doGRVCheck = Promise<Void>();
			// printf("GRV checker waking: %d pending\n", bwData->grvVersion.numWaiting());
		}

		// batch potentially multiple delta files into one GRV, and also rate limit GRVs for this worker
		wait(delay(0.1)); // TODO KNOB?
		// printf("GRV checker doing grv @ %.2f\n", now());

		tr.reset();
		try {
			Version readVersion = wait(tr.getReadVersion());
			ASSERT(readVersion >= bwData->grvVersion.get());
			// printf("GRV checker got GRV %lld\n", readVersion);
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
		self->bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL);
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

					// TODO: pick a reasonable byte limit instead of just piggy-backing
					req.reply.setByteLimit(SERVER_KNOBS->BLOBWORKERSTATUSSTREAM_LIMIT_BYTES);
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
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			self->granuleMetadata.clear();
			throw;
		}
		if (BW_DEBUG) {
			printf("Blob worker got error %s. Exiting...\n", e.name());
		}
		TraceEvent("BlobWorkerDied", self->id).error(e, true);
	}

	wait(self->granuleMetadata.clearAsync());
	return Void();
}

// TODO add unit tests for assign/revoke range, especially version ordering
