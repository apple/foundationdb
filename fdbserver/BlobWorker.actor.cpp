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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/BlobWorkerCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // has to be last include

#define BW_DEBUG false
#define BW_REQUEST_DEBUG false

// TODO add comments + documentation
struct BlobFileIndex {
	Version version;
	std::string filename;
	int64_t offset;
	int64_t length;

	BlobFileIndex() {}

	BlobFileIndex(Version version, std::string filename, int64_t offset, int64_t length)
	  : version(version), filename(filename), offset(offset), length(length) {}
};

struct GranuleFiles {
	std::deque<BlobFileIndex> snapshotFiles;
	std::deque<BlobFileIndex> deltaFiles;
};

struct GranuleHistory {
	KeyRange range;
	Version version;
	Standalone<BlobGranuleHistoryValue> value;

	GranuleHistory() {}

	GranuleHistory(KeyRange range, Version version, Standalone<BlobGranuleHistoryValue> value)
	  : range(range), version(version), value(value) {}
};

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

struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	KeyRange keyRange;

	GranuleFiles files;
	GranuleDeltas currentDeltas; // only contain deltas in pendingDeltaVersion + 1, bufferedDeltaVersion
	// TODO get rid of this and do Reference<Standalone<GranuleDeltas>>?
	Arena deltaArena;

	uint64_t bytesInNewDeltaFiles = 0;
	uint64_t bufferedDeltaBytes = 0;

	// for client to know when it is safe to read a certain version and from where (check waitForVersion)
	NotifiedVersion bufferedDeltaVersion; // largest delta version in currentDeltas (including empty versions)
	Version pendingDeltaVersion = 0; // largest version in progress writing to s3/fdb
	NotifiedVersion durableDeltaVersion; // largest version persisted in s3/fdb
	NotifiedVersion durableSnapshotVersion; // same as delta vars, except for snapshots
	Version pendingSnapshotVersion = 0;

	AsyncVar<int> rollbackCount;

	int64_t originalEpoch;
	int64_t originalSeqno;
	int64_t continueEpoch;
	int64_t continueSeqno;

	Promise<Void> cancelled;
	Promise<Void> readable;
	Promise<Void> historyLoaded;

	Promise<Void> resumeSnapshot;

	AssignBlobRangeRequest originalReq;

	void resume() {
		ASSERT(resumeSnapshot.canBeSet());
		resumeSnapshot.send(Void());
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

	GranuleRangeMetadata() : lastEpoch(0), lastSeqno(0) {}
	GranuleRangeMetadata(int64_t epoch, int64_t seqno, Reference<GranuleMetadata> activeMetadata)
	  : lastEpoch(epoch), lastSeqno(seqno), activeMetadata(activeMetadata) {}
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

	AsyncVar<int> pendingDeltaFileCommitChecks;
	AsyncVar<Version> knownCommittedVersion;
	uint64_t knownCommittedCheckCount = 0;

	PromiseStream<AssignBlobRangeRequest> granuleUpdateErrors;

	BlobWorkerData(UID id, Database db) : id(id), db(db), stats(id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL) {}
	~BlobWorkerData() { printf("Destroying blob worker data for %s\n", id.toString().c_str()); }

	bool managerEpochOk(int64_t epoch) {
		if (epoch < currentManagerEpoch) {
			if (BW_DEBUG) {
				printf("BW %s got request from old epoch %lld, notifying manager it is out of date\n",
				       id.toString().c_str(),
				       epoch);
			}
			return false;
		} else {
			if (epoch > currentManagerEpoch) {
				currentManagerEpoch = epoch;
				if (BW_DEBUG) {
					printf("BW %s found new manager epoch %lld\n", id.toString().c_str(), currentManagerEpoch);
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
			printf("Lock acquire check failed. Proposed (%lld, %lld) < previous (%lld, %lld)\n",
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
			printf("Lock assignment check failed. Expected (%lld, %lld), got (%lld, %lld)\n",
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

// used for "business logic" of both versions of loading granule files
ACTOR Future<Void> readGranuleFiles(Transaction* tr, Key* startKey, Key endKey, GranuleFiles* files, UID granuleID) {

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
	if (BW_DEBUG) {
		printf("Loaded %d snapshot and %d delta files for %s\n",
		       files->snapshotFiles.size(),
		       files->deltaFiles.size(),
		       granuleID.toString().c_str());
	}
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

	RangeResult totalState = wait(tr->getRange(currentRange, 100));
	// TODO is this explicit conflit range necessary with the above read?
	tr->addWriteConflictRange(currentRange);
	ASSERT(!totalState.more);

	if (totalState.empty()) {
		ASSERT(newState == BlobGranuleSplitState::Done);
		if (BW_DEBUG) {
			printf("Found empty split state for parent granule %s\n", parentGranuleID.toString().c_str());
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
		if (st == BlobGranuleSplitState::Started) {
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
			printf("Updating granule %s split state from %s %d -> %d\n",
			       currentGranuleID.toString().c_str(),
			       parentGranuleID.toString().c_str(),
			       currentState,
			       newState);
		}

		Key myStateKey = blobGranuleSplitKeyFor(parentGranuleID, currentGranuleID);
		if (newState == BlobGranuleSplitState::Done && currentState == BlobGranuleSplitState::Assigned &&
		    totalDone == total - 1) {
			// we are the last one to change from Assigned -> Done, so everything can be cleaned up for the old
			// change feed and splitting state
			if (BW_DEBUG) {
				printf("%s destroying old granule %s\n",
				       currentGranuleID.toString().c_str(),
				       parentGranuleID.toString().c_str());
			}
			Key oldGranuleLockKey = blobGranuleLockKeyFor(parentGranuleRange);
			tr->destroyChangeFeed(KeyRef(parentGranuleID.toString()));
			tr->clear(singleKeyRange(oldGranuleLockKey));
			tr->clear(currentRange);
		} else {
			if (newState == BlobGranuleSplitState::Assigned && currentState == BlobGranuleSplitState::Started &&
			    totalStarted == 1) {
				if (BW_DEBUG) {
					printf("%s WOULD BE stopping change feed for old granule %s\n",
					       currentGranuleID.toString().c_str(),
					       parentGranuleID.toString().c_str());
				}
				// FIXME: enable once implemented
				// tr.stopChangeFeed(KeyRef(prevChangeFeedId.toString()));
			}
			tr->atomicOp(myStateKey, blobGranuleSplitValueFor(newState), MutationRef::SetVersionstampedValue);
		}
	} else if (BW_DEBUG) {
		printf("Ignoring granule %s split state from %s %d -> %d\n",
		       currentGranuleID.toString().c_str(),
		       parentGranuleID.toString().c_str(),
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
                                           Arena deltaArena,
                                           GranuleDeltas deltasToWrite,
                                           Version currentDeltaVersion,
                                           Future<BlobFileIndex> previousDeltaFileFuture,
                                           Optional<std::pair<KeyRange, UID>> oldGranuleComplete) {
	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));
	// potentially kick off delta file commit check, if our version isn't already known to be committed
	state uint64_t checkCount = -1;
	if (bwData->knownCommittedVersion.get() < currentDeltaVersion) {
		bwData->pendingDeltaFileCommitChecks.set(bwData->pendingDeltaFileCommitChecks.get() + 1);
		checkCount = bwData->knownCommittedCheckCount;
	}

	// TODO some sort of directory structure would be useful?
	state std::string fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
	                          std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(currentDeltaVersion) +
	                          ".delta";

	state Value serialized = ObjectWriter::toValue(deltasToWrite, Unversioned());

	// FIXME: technically we can free up deltaArena here to reduce memory

	state Reference<IBackupFile> objectFile = wait(bwData->bstore->writeFile(fname));

	++bwData->stats.s3PutReqs;
	++bwData->stats.deltaFilesWritten;
	bwData->stats.deltaBytesWritten += serialized.size();

	wait(objectFile->append(serialized.begin(), serialized.size()));
	wait(objectFile->finish());

	state int numIterations = 0;
	try {
		// before updating FDB, wait for the delta file version to be committed and previous delta files to finish
		while (bwData->knownCommittedVersion.get() < currentDeltaVersion) {
			if (bwData->knownCommittedCheckCount != checkCount) {
				checkCount = bwData->knownCommittedCheckCount;
				// a check happened between the start and now, and the version is still lower. Kick off another one.
				bwData->pendingDeltaFileCommitChecks.set(bwData->pendingDeltaFileCommitChecks.get() + 1);
			}
			wait(bwData->knownCommittedVersion.onChange());
		}
		BlobFileIndex prev = wait(previousDeltaFileFuture);
		wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));

		// update FDB with new file
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));

				Key dfKey = blobGranuleFileKeyFor(granuleID, 'D', currentDeltaVersion);
				Value dfValue = blobGranuleFileValueFor(fname, 0, serialized.size());
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
					printf("Granule %s [%s - %s) updated fdb with delta file %s of size %d at version %lld, cv=%lld\n",
					       granuleID.toString().c_str(),
					       keyRange.begin.printable().c_str(),
					       keyRange.end.printable().c_str(),
					       fname.c_str(),
					       serialized.size(),
					       currentDeltaVersion,
					       tr->getCommittedVersion());
				}

				if (BUGGIFY_WITH_PROB(0.01)) {
					wait(delay(deterministicRandom()->random01()));
				}
				return BlobFileIndex(currentDeltaVersion, fname, 0, serialized.size());
			} catch (Error& e) {
				numIterations++;
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}

		// if commit failed the first time due to granule assignment conflict (which is non-retryable),
		// then the file key was persisted and we should delete it. Otherwise, the commit failed
		// for some other reason and the key wasn't persisted, so we should just propogate the error
		if (numIterations != 1 || e.code() != error_code_granule_assignment_conflict) {
			throw e;
		}

		if (BW_DEBUG) {
			printf("deleting s3 delta file %s after error %s\n", fname.c_str(), e.name());
		}
		state Error eState = e;
		++bwData->stats.s3DeleteReqs;
		wait(bwData->bstore->deleteFile(fname));
		throw eState;
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
	// TODO some sort of directory structure would be useful maybe?
	state std::string fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
	                          std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(version) + ".snapshot";
	state Arena arena;
	state GranuleSnapshot snapshot;

	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

	loop {
		try {
			RangeResult res = waitNext(rows.getFuture());
			arena.dependsOn(res.arena());
			snapshot.append(arena, res.begin(), res.size());
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
		printf("Granule [%s - %s) read %d snapshot rows\n",
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str(),
		       snapshot.size());
	}

	// TODO REMOVE sanity checks!
	if (snapshot.size() > 0) {
		ASSERT(keyRange.begin <= snapshot[0].key);
		ASSERT(keyRange.end > snapshot[snapshot.size() - 1].key);
	}
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

	++bwData->stats.s3PutReqs;
	++bwData->stats.snapshotFilesWritten;
	bwData->stats.snapshotBytesWritten += serialized.size();

	// TODO: inject write error
	wait(objectFile->append(serialized.begin(), serialized.size()));
	wait(objectFile->finish());

	wait(delay(0, TaskPriority::BlobWorkerUpdateFDB));
	// object uploaded successfully, save it to system key space

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bwData->db);
	state int numIterations = 0;

	try {
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			try {
				wait(readAndCheckGranuleLock(tr, keyRange, epoch, seqno));
				Key snapshotFileKey = blobGranuleFileKeyFor(granuleID, 'S', version);
				Key snapshotFileValue = blobGranuleFileValueFor(fname, 0, serialized.size());
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
				numIterations++;
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}

		// if commit failed the first time due to granule assignment conflict (which is non-retryable),
		// then the file key was persisted and we should delete it. Otherwise, the commit failed
		// for some other reason and the key wasn't persisted, so we should just propogate the error
		if (numIterations != 1 || e.code() != error_code_granule_assignment_conflict) {
			throw e;
		}

		if (BW_DEBUG) {
			printf("deleting s3 snapshot file %s after error %s\n", fname.c_str(), e.name());
		}
		state Error eState = e;
		++bwData->stats.s3DeleteReqs;
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

	if (BUGGIFY_WITH_PROB(0.1)) {
		wait(delay(deterministicRandom()->random01()));
	}

	return BlobFileIndex(version, fname, 0, serialized.size());
}

ACTOR Future<BlobFileIndex> dumpInitialSnapshotFromFDB(Reference<BlobWorkerData> bwData,
                                                       Reference<GranuleMetadata> metadata,
                                                       UID granuleID) {
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
			DEBUG_KEY_RANGE("BlobWorkerFDBSnapshot", readVersion, metadata->keyRange, bwData->id);
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

// files might not be the current set of files in metadata, in the case of doing the initial snapshot of a granule that
// was split.
ACTOR Future<BlobFileIndex> compactFromBlob(Reference<BlobWorkerData> bwData,
                                            Reference<GranuleMetadata> metadata,
                                            UID granuleID,
                                            GranuleFiles files) {
	wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));
	if (BW_DEBUG) {
		printf("Compacting snapshot from blob for [%s - %s)\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str());
	}

	ASSERT(!files.snapshotFiles.empty());
	ASSERT(!files.deltaFiles.empty());
	state Version version = files.deltaFiles.back().version;

	state Arena filenameArena;
	state BlobGranuleChunkRef chunk;

	state int64_t compactBytesRead = 0;
	state Version snapshotVersion = files.snapshotFiles.back().version;
	BlobFileIndex snapshotF = files.snapshotFiles.back();
	chunk.snapshotFile = BlobFilePointerRef(filenameArena, snapshotF.filename, snapshotF.offset, snapshotF.length);
	compactBytesRead += snapshotF.length;
	int deltaIdx = files.deltaFiles.size() - 1;
	while (deltaIdx >= 0 && files.deltaFiles[deltaIdx].version > snapshotVersion) {
		deltaIdx--;
	}
	deltaIdx++;
	while (deltaIdx < files.deltaFiles.size()) {
		BlobFileIndex deltaF = files.deltaFiles[deltaIdx];
		chunk.deltaFiles.emplace_back_deep(filenameArena, deltaF.filename, deltaF.offset, deltaF.length);
		compactBytesRead += deltaF.length;
		deltaIdx++;
	}
	chunk.includedVersion = version;

	if (BW_DEBUG) {
		printf("Re-snapshotting [%s - %s) @ %lld from blob\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str(),
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
				printf("Compacting snapshot from blob for [%s - %s) got error %s\n",
				       metadata->keyRange.begin.printable().c_str(),
				       metadata->keyRange.end.printable().c_str(),
				       e.name());
			}
			throw e;
		}
	}
}

// When reading from a prior change feed, the prior change feed may contain mutations that don't belong in the new
// granule. And, we only want to read the prior change feed up to the start of the new change feed.
static bool filterOldMutations(const KeyRange& range,
                               const Standalone<VectorRef<MutationsAndVersionRef>>* oldMutations,
                               Standalone<VectorRef<MutationsAndVersionRef>>* mutations,
                               Version maxVersion) {
	Standalone<VectorRef<MutationsAndVersionRef>> filteredMutations;
	mutations->arena().dependsOn(range.arena());
	mutations->arena().dependsOn(oldMutations->arena());
	for (auto& delta : *oldMutations) {
		if (delta.version >= maxVersion) {
			return true;
		}
		MutationsAndVersionRef filteredDelta;
		filteredDelta.version = delta.version;
		for (auto& m : delta.mutations) {
			ASSERT(m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange);
			if (m.type == MutationRef::SetValue) {
				if (m.param1 >= range.begin && m.param1 < range.end) {
					filteredDelta.mutations.push_back(mutations->arena(), m);
				}
			} else {
				if (m.param2 >= range.begin && m.param1 < range.end) {
					// clamp clear range down to sub-range
					MutationRef m2 = m;
					if (range.begin > m.param1) {
						m2.param1 = range.begin;
					}
					if (range.end < m.param2) {
						m2.param2 = range.end;
					}
					filteredDelta.mutations.push_back(mutations->arena(), m2);
				}
			}
		}
		mutations->push_back(mutations->arena(), filteredDelta);
	}
	return false;
}

ACTOR Future<Void> handleCompletedDeltaFile(Reference<BlobWorkerData> bwData,
                                            Reference<GranuleMetadata> metadata,
                                            BlobFileIndex completedDeltaFile,
                                            Key cfKey,
                                            Version cfStartVersion,
                                            std::deque<std::pair<Version, Version>> rollbacksInProgress) {
	metadata->files.deltaFiles.push_back(completedDeltaFile);
	ASSERT(metadata->durableDeltaVersion.get() < completedDeltaFile.version);
	metadata->durableDeltaVersion.set(completedDeltaFile.version);

	if (completedDeltaFile.version > cfStartVersion) {
		if (BW_DEBUG) {
			printf("Popping change feed %s at %lld\n", cfKey.printable().c_str(), completedDeltaFile.version);
		}
		// FIXME: for a write-hot shard, we could potentially batch these and only pop the largest one after several
		// have completed
		// FIXME: also have these be async, have each pop change feed wait on the prior one, wait on them before
		// re-snapshotting
		Future<Void> popFuture = bwData->db->popChangeFeedMutations(cfKey, completedDeltaFile.version);
		wait(popFuture);
	}
	while (!rollbacksInProgress.empty() && completedDeltaFile.version >= rollbacksInProgress.front().first) {
		rollbacksInProgress.pop_front();
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

struct InFlightDeltaFile {
	Future<BlobFileIndex> future;
	Version version;
	uint64_t bytes;

	InFlightDeltaFile(Future<BlobFileIndex> future, Version version, uint64_t bytes)
	  : future(future), version(version), bytes(bytes) {}
};

static Version doGranuleRollback(Reference<GranuleMetadata> metadata,
                                 Version mutationVersion,
                                 Version rollbackVersion,
                                 std::deque<InFlightDeltaFile>& inFlightDeltaFiles,
                                 std::deque<std::pair<Version, Version>>& rollbacksInProgress,
                                 std::deque<std::pair<Version, Version>>& rollbacksCompleted) {
	Version cfRollbackVersion;
	if (metadata->pendingDeltaVersion > rollbackVersion) {
		// if we already started writing mutations to a delta file with version > rollbackVersion,
		// we need to rescind those delta file writes
		ASSERT(!inFlightDeltaFiles.empty());
		cfRollbackVersion = metadata->durableDeltaVersion.get();
		int toPop = 0;
		for (auto& df : inFlightDeltaFiles) {
			if (df.version > rollbackVersion) {
				df.future.cancel();
				metadata->bytesInNewDeltaFiles -= df.bytes;
				toPop++;
				if (BW_DEBUG) {
					printf("[%s - %s) rollback cancelling delta file @ %lld\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       df.version);
				}
			} else {
				ASSERT(df.version > cfRollbackVersion);
				cfRollbackVersion = df.version;
			}
		}
		ASSERT(toPop > 0);
		while (toPop > 0) {
			inFlightDeltaFiles.pop_back();
			toPop--;
		}
		metadata->pendingDeltaVersion = cfRollbackVersion;
		if (BW_DEBUG) {
			printf("[%s - %s) rollback discarding all %d in-memory mutations\n",
			       metadata->keyRange.begin.printable().c_str(),
			       metadata->keyRange.end.printable().c_str(),
			       metadata->currentDeltas.size());
		}

		// discard all in-memory mutations
		metadata->deltaArena = Arena();
		metadata->currentDeltas = GranuleDeltas();
		metadata->bufferedDeltaBytes = 0;
		metadata->bufferedDeltaVersion.set(cfRollbackVersion);

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
			printf("[%s - %s) rollback discarding %d in-memory mutations, %d mutations and %lld bytes left\n",
			       metadata->keyRange.begin.printable().c_str(),
			       metadata->keyRange.end.printable().c_str(),
			       metadata->currentDeltas.size() - mIdx,
			       mIdx,
			       metadata->bufferedDeltaBytes);
		}

		metadata->currentDeltas.resize(metadata->deltaArena, mIdx);

		// delete all deltas in rollback range, but we can optimize here to just skip the uncommitted mutations
		// directly and immediately pop the rollback out of inProgress
		metadata->bufferedDeltaVersion.set(rollbackVersion);
		cfRollbackVersion = mutationVersion;
	}

	if (BW_DEBUG) {
		printf("[%s - %s) finishing rollback to %lld\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str(),
		       cfRollbackVersion);
	}

	metadata->rollbackCount.set(metadata->rollbackCount.get() + 1);

	// add this rollback to in progress, and put all completed ones back in progress
	rollbacksInProgress.push_back(std::pair(rollbackVersion, mutationVersion));
	for (int i = rollbacksCompleted.size() - 1; i >= 0; i--) {
		rollbacksInProgress.push_front(rollbacksCompleted[i]);
	}
	rollbacksCompleted.clear();

	return cfRollbackVersion;
}

// updater for a single granule
// TODO: this is getting kind of large. Should try to split out this actor if it continues to grow?
// FIXME: handle errors here (forward errors)
ACTOR Future<Void> blobGranuleUpdateFiles(Reference<BlobWorkerData> bwData,
                                          Reference<GranuleMetadata> metadata,
                                          Future<GranuleStartState> assignFuture) {
	state PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> oldChangeFeedStream;
	state PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> changeFeedStream;
	state Future<BlobFileIndex> inFlightBlobSnapshot;
	state std::deque<InFlightDeltaFile> inFlightDeltaFiles;
	state Future<Void> oldChangeFeedFuture;
	state Future<Void> changeFeedFuture;
	state GranuleStartState startState;
	state bool readOldChangeFeed;
	state bool lastFromOldChangeFeed = false;
	state Optional<std::pair<KeyRange, UID>> oldChangeFeedDataComplete;
	state Key cfKey;
	state Optional<Key> oldCFKey;

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
			printf("Granule File Updater Starting for [%s - %s):\n",
			       metadata->keyRange.begin.printable().c_str(),
			       metadata->keyRange.end.printable().c_str());
			printf("  CFID: %s\n", startState.granuleID.toString().c_str());
			printf("  CF Start Version: %lld\n", startState.changeFeedStartVersion);
			printf("  Previous Durable Version: %lld\n", startState.previousDurableVersion);
			printf("  doSnapshot=%s\n", startState.doSnapshot ? "T" : "F");
			printf("  Prev CFID: %s\n",
			       startState.parentGranule.present() ? startState.parentGranule.get().second.toString().c_str() : "");
			printf("  blobFilesToSnapshot=%s\n", startState.blobFilesToSnapshot.present() ? "T" : "F");
		}

		state Version startVersion;
		state BlobFileIndex newSnapshotFile;

		inFlightBlobSnapshot = Future<BlobFileIndex>(); // not valid!

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
		} else {
			if (startState.blobFilesToSnapshot.present()) {
				inFlightBlobSnapshot =
				    compactFromBlob(bwData, metadata, startState.granuleID, startState.blobFilesToSnapshot.get());
				startVersion = startState.previousDurableVersion;
				metadata->durableSnapshotVersion.set(startState.blobFilesToSnapshot.get().snapshotFiles.back().version);
			} else {
				ASSERT(startState.previousDurableVersion == invalidVersion);
				BlobFileIndex fromFDB = wait(dumpInitialSnapshotFromFDB(bwData, metadata, startState.granuleID));
				newSnapshotFile = fromFDB;
				ASSERT(startState.changeFeedStartVersion <= fromFDB.version);
				startVersion = newSnapshotFile.version;
				metadata->files.snapshotFiles.push_back(newSnapshotFile);
				metadata->durableSnapshotVersion.set(startVersion);

				// construct fake history entry so we can store start version for splitting later
				startState.history =
				    GranuleHistory(metadata->keyRange, startVersion, Standalone<BlobGranuleHistoryValue>());

				wait(yield(TaskPriority::BlobWorkerUpdateStorage));
			}
			metadata->pendingSnapshotVersion = startVersion;
		}

		metadata->durableDeltaVersion.set(startVersion);
		metadata->pendingDeltaVersion = startVersion;
		metadata->bufferedDeltaVersion.set(startVersion);

		ASSERT(metadata->readable.canBeSet());
		metadata->readable.send(Void());

		if (startState.parentGranule.present()) {
			// FIXME: once we have empty versions, only include up to startState.changeFeedStartVersion in the read
			// stream. Then we can just stop the old stream when we get end_of_stream from this and not handle the
			// mutation version truncation stuff

			// FIXME: filtering on key range != change feed range doesn't work
			readOldChangeFeed = true;
			oldChangeFeedFuture =
			    bwData->db->getChangeFeedStream(oldChangeFeedStream,
			                                    oldCFKey.get(),
			                                    startVersion + 1,
			                                    MAX_VERSION,
			                                    startState.parentGranule.get().first /*metadata->keyRange*/);
		} else {
			readOldChangeFeed = false;
			changeFeedFuture = bwData->db->getChangeFeedStream(
			    changeFeedStream, cfKey, startVersion + 1, MAX_VERSION, metadata->keyRange);
		}

		state Version lastVersion = startVersion + 1;
		loop {
			// check outstanding snapshot/delta files for completion
			if (inFlightBlobSnapshot.isValid() && inFlightBlobSnapshot.isReady()) {
				BlobFileIndex completedSnapshot = wait(inFlightBlobSnapshot);
				metadata->files.snapshotFiles.push_back(completedSnapshot);
				metadata->durableSnapshotVersion.set(completedSnapshot.version);
				inFlightBlobSnapshot = Future<BlobFileIndex>(); // not valid!
				if (BW_DEBUG) {
					printf("Async Blob Snapshot completed for [%s - %s)\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str());
				}

				wait(yield(TaskPriority::BlobWorkerUpdateStorage));
			}
			if (!inFlightBlobSnapshot.isValid()) {
				while (inFlightDeltaFiles.size() > 0) {
					if (inFlightDeltaFiles.front().future.isReady()) {
						BlobFileIndex completedDeltaFile = wait(inFlightDeltaFiles.front().future);
						wait(handleCompletedDeltaFile(bwData,
						                              metadata,
						                              completedDeltaFile,
						                              cfKey,
						                              startState.changeFeedStartVersion,
						                              rollbacksCompleted));

						inFlightDeltaFiles.pop_front();
						wait(yield(TaskPriority::BlobWorkerUpdateStorage));
					} else {
						break;
					}
				}
			}

			// inject delay into reading change feed stream
			if (BUGGIFY_WITH_PROB(0.001)) {
				wait(delay(deterministicRandom()->random01(), TaskPriority::BlobWorkerReadChangeFeed));
			} else {
				wait(delay(0, TaskPriority::BlobWorkerReadChangeFeed));
			}

			state Standalone<VectorRef<MutationsAndVersionRef>> mutations;
			if (readOldChangeFeed) {
				Standalone<VectorRef<MutationsAndVersionRef>> oldMutations = waitNext(oldChangeFeedStream.getFuture());
				// TODO filter old mutations won't be necessary, SS does it already
				if (filterOldMutations(
				        metadata->keyRange, &oldMutations, &mutations, startState.changeFeedStartVersion)) {
					// if old change feed has caught up with where new one would start, finish last one and start new
					// one

					Key cfKey = StringRef(startState.granuleID.toString());
					changeFeedFuture = bwData->db->getChangeFeedStream(
					    changeFeedStream, cfKey, startState.changeFeedStartVersion, MAX_VERSION, metadata->keyRange);
					oldChangeFeedFuture.cancel();
					lastFromOldChangeFeed = true;

					// now that old change feed is cancelled, clear out any mutations still in buffer by replacing
					// promise stream
					oldChangeFeedStream = PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>>();
				}
			} else {
				Standalone<VectorRef<MutationsAndVersionRef>> newMutations = waitNext(changeFeedStream.getFuture());
				mutations = newMutations;
			}

			// process mutations
			for (MutationsAndVersionRef d : mutations) {
				state MutationsAndVersionRef deltas = d;
				ASSERT(deltas.version >= lastVersion);
				ASSERT(lastVersion > metadata->bufferedDeltaVersion.get());

				// if lastVersion is complete, update buffered version and potentially write a delta file with
				// everything up to lastVersion
				if (deltas.version > lastVersion) {
					metadata->bufferedDeltaVersion.set(lastVersion);
				}
				// Write a new delta file IF we have enough bytes, and we have all of the previous version's stuff
				// there to ensure no versions span multiple delta files. Check this by ensuring the version of this
				// new delta is larger than the previous largest seen version
				if (metadata->bufferedDeltaBytes >= SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES &&
				    deltas.version > lastVersion) {
					if (BW_DEBUG) {
						printf("Granule [%s - %s) flushing delta file after %d bytes @ %lld %lld%s\n",
						       metadata->keyRange.begin.printable().c_str(),
						       metadata->keyRange.end.printable().c_str(),
						       metadata->bufferedDeltaBytes,
						       lastVersion,
						       deltas.version,
						       oldChangeFeedDataComplete.present() ? ". Finalizing " : "");
					}
					TraceEvent("BlobGranuleDeltaFile", bwData->id)
					    .detail("Granule", metadata->keyRange)
					    .detail("Version", lastVersion);

					// sanity check for version order
					ASSERT(lastVersion >= metadata->currentDeltas.back().version);
					ASSERT(metadata->pendingDeltaVersion < metadata->currentDeltas.front().version);

					// launch pipelined, but wait for previous operation to complete before persisting to FDB
					Future<BlobFileIndex> previousDeltaFileFuture;
					if (inFlightBlobSnapshot.isValid() && inFlightDeltaFiles.empty()) {
						previousDeltaFileFuture = inFlightBlobSnapshot;
					} else if (!inFlightDeltaFiles.empty()) {
						previousDeltaFileFuture = inFlightDeltaFiles.back().future;
					} else {
						previousDeltaFileFuture = Future<BlobFileIndex>(BlobFileIndex());
					}
					Future<BlobFileIndex> dfFuture = writeDeltaFile(bwData,
					                                                metadata->keyRange,
					                                                startState.granuleID,
					                                                metadata->originalEpoch,
					                                                metadata->originalSeqno,
					                                                metadata->deltaArena,
					                                                metadata->currentDeltas,
					                                                lastVersion,
					                                                previousDeltaFileFuture,
					                                                oldChangeFeedDataComplete);
					inFlightDeltaFiles.push_back(
					    InFlightDeltaFile(dfFuture, lastVersion, metadata->bufferedDeltaBytes));

					oldChangeFeedDataComplete.reset();
					// add new pending delta file
					ASSERT(metadata->pendingDeltaVersion < lastVersion);
					metadata->pendingDeltaVersion = lastVersion;
					metadata->bytesInNewDeltaFiles += metadata->bufferedDeltaBytes;

					bwData->stats.mutationBytesBuffered -= metadata->bufferedDeltaBytes;

					// reset current deltas
					metadata->deltaArena = Arena();
					metadata->currentDeltas = GranuleDeltas();
					metadata->bufferedDeltaBytes = 0;

					// if we just wrote a delta file, check if we need to compact here.
					// exhaust old change feed before compacting - otherwise we could end up with an endlessly
					// growing list of previous change feeds in the worst case.
					snapshotEligible = true;
				}

				// FIXME: if we're still reading from old change feed, we should probably compact if we're making a
				// bunch of extra delta files at some point, even if we don't consider it for a split yet
				if (snapshotEligible && metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT &&
				    !readOldChangeFeed) {
					if (BW_DEBUG && (inFlightBlobSnapshot.isValid() || !inFlightDeltaFiles.empty())) {
						printf("Granule [%s - %s) ready to re-snapshot, waiting for outstanding %d snapshot and %d "
						       "deltas to "
						       "finish\n",
						       metadata->keyRange.begin.printable().c_str(),
						       metadata->keyRange.end.printable().c_str(),
						       inFlightBlobSnapshot.isValid() ? 1 : 0,
						       inFlightDeltaFiles.size());
					}
					// wait for all in flight snapshot/delta files
					if (inFlightBlobSnapshot.isValid()) {
						BlobFileIndex completedSnapshot = wait(inFlightBlobSnapshot);
						metadata->files.snapshotFiles.push_back(completedSnapshot);
						metadata->durableSnapshotVersion.set(completedSnapshot.version);
						inFlightBlobSnapshot = Future<BlobFileIndex>(); // not valid!
						wait(yield(TaskPriority::BlobWorkerUpdateStorage));
					}
					for (auto& it : inFlightDeltaFiles) {
						BlobFileIndex completedDeltaFile = wait(it.future);
						wait(handleCompletedDeltaFile(bwData,
						                              metadata,
						                              completedDeltaFile,
						                              cfKey,
						                              startState.changeFeedStartVersion,
						                              rollbacksCompleted));
						wait(yield(TaskPriority::BlobWorkerUpdateStorage));
					}
					inFlightDeltaFiles.clear();

					if (BW_DEBUG) {
						printf("Granule [%s - %s) checking with BM for re-snapshot after %d bytes\n",
						       metadata->keyRange.begin.printable().c_str(),
						       metadata->keyRange.end.printable().c_str(),
						       metadata->bytesInNewDeltaFiles);
					}

					TraceEvent("BlobGranuleSnapshotCheck", bwData->id)
					    .detail("Granule", metadata->keyRange)
					    .detail("Version", metadata->durableDeltaVersion.get());

					// Save these from the start so repeated requests are idempotent
					// Need to retry in case response is dropped or manager changes. Eventually, a manager will
					// either reassign the range with continue=true, or will revoke the range. But, we will keep the
					// range open at this version for reads until that assignment change happens
					metadata->resumeSnapshot.reset();
					state int64_t statusEpoch = metadata->continueEpoch;
					state int64_t statusSeqno = metadata->continueSeqno;
					loop {
						loop {
							try {
								wait(bwData->currentManagerStatusStream.get().onReady());
								bwData->currentManagerStatusStream.get().send(
								    GranuleStatusReply(metadata->keyRange,
								                       true,
								                       statusEpoch,
								                       statusSeqno,
								                       startState.granuleID,
								                       startState.history.get().version,
								                       metadata->durableDeltaVersion.get()));
								break;
							} catch (Error& e) {
								wait(bwData->currentManagerStatusStream.onChange());
							}
						}

						choose {
							when(wait(metadata->resumeSnapshot.getFuture())) { break; }
							when(wait(delay(1.0))) {}
							when(wait(bwData->currentManagerStatusStream.onChange())) {}
						}

						if (BW_DEBUG) {
							printf("Granule [%s - %s)\n, hasn't heard back from BM in BW %s, re-sending status\n",
							       metadata->keyRange.begin.printable().c_str(),
							       metadata->keyRange.end.printable().c_str(),
							       bwData->id.toString().c_str());
						}
					}

					if (BW_DEBUG) {
						printf("Granule [%s - %s) re-snapshotting after %d bytes\n",
						       metadata->keyRange.begin.printable().c_str(),
						       metadata->keyRange.end.printable().c_str(),
						       metadata->bytesInNewDeltaFiles);
					}
					TraceEvent("BlobGranuleSnapshotFile", bwData->id)
					    .detail("Granule", metadata->keyRange)
					    .detail("Version", metadata->durableDeltaVersion.get());
					// TODO: this could read from FDB instead if it knew there was a large range clear at the end or
					// it knew the granule was small, or something

					// Have to copy files object so that adding to it as we start writing new delta files in
					// parallel doesn't conflict. We could also pass the snapshot version and ignore any snapshot
					// files >= version and any delta files > version, but that's more complicated
					inFlightBlobSnapshot = compactFromBlob(bwData, metadata, startState.granuleID, metadata->files);
					metadata->pendingSnapshotVersion = metadata->durableDeltaVersion.get();

					// reset metadata
					metadata->bytesInNewDeltaFiles = 0;
				} else if (snapshotEligible &&
				           metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
					// if we're in the old change feed case and can't snapshot but we have enough data to, don't
					// queue too many delta files in parallel
					while (inFlightDeltaFiles.size() > 10) {
						if (BW_DEBUG) {
							printf("[%s - %s) Waiting on delta file b/c old change feed\n",
							       metadata->keyRange.begin.printable().c_str(),
							       metadata->keyRange.end.printable().c_str());
						}
						BlobFileIndex completedDeltaFile = wait(inFlightDeltaFiles.front().future);
						if (BW_DEBUG) {
							printf("  [%s - %s) Got completed delta file\n",
							       metadata->keyRange.begin.printable().c_str(),
							       metadata->keyRange.end.printable().c_str());
						}
						wait(handleCompletedDeltaFile(bwData,
						                              metadata,
						                              completedDeltaFile,
						                              cfKey,
						                              startState.changeFeedStartVersion,
						                              rollbacksCompleted));
						wait(yield(TaskPriority::BlobWorkerUpdateStorage));
						inFlightDeltaFiles.pop_front();
					}
				}
				snapshotEligible = false;

				wait(yield(TaskPriority::BlobWorkerReadChangeFeed));

				// finally, after we optionally write delta and snapshot files, add new mutations to buffer
				if (!deltas.mutations.empty()) {
					if (deltas.mutations.size() == 1 && deltas.mutations.back().param1 == lastEpochEndPrivateKey) {
						// Note rollbackVerision is durable, [rollbackVersion+1 - deltas.version] needs to be tossed
						// For correctness right now, there can be no waits and yields either in rollback handling
						// or in handleBlobGranuleFileRequest once waitForVersion has succeeded, otherwise this will
						// race and clobber results
						Version rollbackVersion;
						BinaryReader br(deltas.mutations[0].param2, Unversioned());
						br >> rollbackVersion;

						// FIXME: THIS IS FALSE!! delta can commit by getting committed version out of band, without
						// seeing rollback mutation.
						ASSERT(rollbackVersion >= metadata->durableDeltaVersion.get());

						if (!rollbacksInProgress.empty()) {
							ASSERT(rollbacksInProgress.front().first == rollbackVersion);
							ASSERT(rollbacksInProgress.front().second == deltas.version);
							printf("Passed rollback %lld -> %lld\n", deltas.version, rollbackVersion);
							rollbacksCompleted.push_back(rollbacksInProgress.front());
							rollbacksInProgress.pop_front();
						} else {
							// FIXME: add counter for granule rollbacks and rollbacks skipped?
							// explicitly check last delta in currentDeltas because lastVersion and bufferedDeltaVersion
							// include empties
							if (metadata->pendingDeltaVersion <= rollbackVersion &&
							    (metadata->currentDeltas.empty() ||
							     metadata->currentDeltas.back().version <= rollbackVersion)) {

								if (BW_DEBUG) {
									printf("BW skipping rollback %lld -> %lld completely\n",
									       deltas.version,
									       rollbackVersion);
								}
							} else {
								if (BW_DEBUG) {
									printf("BW [%s - %s) ROLLBACK @ %lld -> %lld\n",
									       metadata->keyRange.begin.printable().c_str(),
									       metadata->keyRange.end.printable().c_str(),
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
								                                              inFlightDeltaFiles,
								                                              rollbacksInProgress,
								                                              rollbacksCompleted);

								// reset change feeds to cfRollbackVersion
								if (readOldChangeFeed) {
									oldChangeFeedStream =
									    PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>>();
									oldChangeFeedFuture = bwData->db->getChangeFeedStream(
									    oldChangeFeedStream,
									    oldCFKey.get(),
									    cfRollbackVersion + 1,
									    MAX_VERSION,
									    startState.parentGranule.get().first /*metadata->keyRange*/);
								} else {
									changeFeedStream = PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>>();
									changeFeedFuture = bwData->db->getChangeFeedStream(changeFeedStream,
									                                                   cfKey,
									                                                   cfRollbackVersion + 1,
									                                                   MAX_VERSION,
									                                                   metadata->keyRange);
								}
								justDidRollback = true;
								break;
							}
						}
					} else if (!rollbacksInProgress.empty() && rollbacksInProgress.front().first < deltas.version &&
					           rollbacksInProgress.front().second > deltas.version) {
						if (BW_DEBUG) {
							printf("Skipping mutations @ %lld b/c prior rollback\n", deltas.version);
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
						metadata->currentDeltas.push_back_deep(metadata->deltaArena, deltas);
					}
				}
				if (justDidRollback) {
					break;
				}
				lastVersion = deltas.version;
			}
			if (lastFromOldChangeFeed && !justDidRollback) {
				readOldChangeFeed = false;
				lastFromOldChangeFeed = false;
				// set this so next delta file write updates granule split metadata to done
				ASSERT(startState.parentGranule.present());
				oldChangeFeedDataComplete = startState.parentGranule.get();
				if (BW_DEBUG) {
					printf("Granule [%s - %s) switching to new change feed %s @ %lld\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       startState.granuleID.toString().c_str(),
					       metadata->bufferedDeltaVersion.get());
				}
			}
			justDidRollback = false;
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw;
		}

		if (metadata->cancelled.canBeSet()) {
			metadata->cancelled.send(Void());
		}

		if (e.code() == error_code_granule_assignment_conflict) {
			TraceEvent(SevInfo, "GranuleAssignmentConflict", bwData->id).detail("Granule", metadata->keyRange);
		} else {
			++bwData->stats.granuleUpdateErrors;
			if (BW_DEBUG) {
				printf("Granule file updater for [%s - %s) got error %s, exiting\n",
				       metadata->keyRange.begin.printable().c_str(),
				       metadata->keyRange.end.printable().c_str(),
				       e.name());
			}
			TraceEvent(SevWarn, "GranuleFileUpdaterError", bwData->id).detail("Granule", metadata->keyRange).error(e);

			if (granuleCanRetry(e)) {
				// explicitly cancel all outstanding write futures BEFORE updating promise stream, to ensure they
				// can't update files after the re-assigned granule acquires the lock
				inFlightBlobSnapshot.cancel();
				for (auto& f : inFlightDeltaFiles) {
					f.future.cancel();
				}

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

			// while the start version of the current granule's parent is larger than the last known start version, walk
			// backwards
			while (curHistory.value.parentGranules.size() > 0 &&
			       curHistory.value.parentGranules[0].second > stopVersion) {
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
				printf("Loaded %d history entries for granule [%s - %s) (%d skipped)\n",
				       historyEntryStack.size(),
				       metadata->keyRange.begin.printable().c_str(),
				       metadata->keyRange.end.printable().c_str(),
				       skipped);
			}
		}

		metadata->historyLoaded.send(Void());
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled || e.code() == error_code_granule_assignment_conflict) {
			throw e;
		}
		if (BW_DEBUG) {
			printf("Loading blob granule history got unexpected error %s\n", e.name());
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

	/*printf("  [%s - %s) waiting for %lld\n  readable:%s\n  bufferedDelta=%lld\n  pendingDelta=%lld\n  "
	       "durableDelta=%lld\n  pendingSnapshot=%lld\n  durableSnapshot=%lld\n",
	       metadata->keyRange.begin.printable().c_str(),
	       metadata->keyRange.end.printable().c_str(),
	       v,
	       metadata->readable.isSet() ? "T" : "F",
	       metadata->bufferedDeltaVersion.get(),
	       metadata->pendingDeltaVersion,
	       metadata->durableDeltaVersion.get(),
	       metadata->pendingSnapshotVersion,
	       metadata->durableSnapshotVersion.get());*/

	if (v <= metadata->bufferedDeltaVersion.get() &&
	    (v <= metadata->durableDeltaVersion.get() ||
	     metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion) &&
	    (v <= metadata->durableSnapshotVersion.get() ||
	     metadata->durableSnapshotVersion.get() == metadata->pendingSnapshotVersion)) {
		return Void();
	}

	// wait for change feed version to catch up to ensure we have all data
	if (metadata->bufferedDeltaVersion.get() < v) {
		wait(metadata->bufferedDeltaVersion.whenAtLeast(v));
	}

	// wait for any pending delta and snapshot files as of the momemt the change feed version caught up.
	state Version pendingDeltaV = metadata->pendingDeltaVersion;
	state Version pendingSnapshotV = metadata->pendingSnapshotVersion;

	ASSERT(pendingDeltaV <= metadata->bufferedDeltaVersion.get());
	if (pendingDeltaV > metadata->durableDeltaVersion.get()) {
		wait(metadata->durableDeltaVersion.whenAtLeast(pendingDeltaV));
	}

	// This isn't strictly needed, but if we're in the process of re-snapshotting, we'd likely rather return that
	// snapshot file than the previous snapshot file and all its delta files.
	if (pendingSnapshotV > metadata->durableSnapshotVersion.get()) {
		wait(metadata->durableSnapshotVersion.whenAtLeast(pendingSnapshotV));
	}

	// There is a race here - we wait for pending delta files before this to finish, but while we do, we kick off
	// another delta file and roll the mutations. In that case, we must return the new delta file instead of in
	// memory mutations, so we wait for that delta file to complete

	if (metadata->pendingDeltaVersion != pendingDeltaV) {
		wait(metadata->durableDeltaVersion.whenAtLeast(pendingDeltaV + 1));
	}

	return Void();
}

ACTOR Future<Void> handleBlobGranuleFileRequest(Reference<BlobWorkerData> bwData, BlobGranuleFileRequest req) {
	try {
		// TODO REMOVE in api V2
		ASSERT(req.beginVersion == 0);
		state BlobGranuleFileReply rep;
		state std::vector<Reference<GranuleMetadata>> granules;

		auto checkRanges = bwData->granuleMetadata.intersectingRanges(req.keyRange);
		// check for gaps as errors and copy references to granule metadata before yielding or doing any work
		KeyRef lastRangeEnd = req.keyRange.begin;

		for (auto& r : checkRanges) {
			bool isValid = r.value().activeMetadata.isValid();
			if (lastRangeEnd < r.begin() || !isValid) {
				if (BW_REQUEST_DEBUG) {
					printf("No %s blob data for [%s - %s) in request range [%s - %s), skipping request\n",
					       isValid ? "" : "valid",
					       lastRangeEnd.printable().c_str(),
					       r.begin().printable().c_str(),
					       req.keyRange.begin.printable().c_str(),
					       req.keyRange.end.printable().c_str());
				}

				throw wrong_shard_server();
			}
			granules.push_back(r.value().activeMetadata);
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

			throw wrong_shard_server();
		}

		// do work for each range
		state Key readThrough = req.keyRange.begin;
		for (auto m : granules) {
			if (readThrough >= m->keyRange.end) {
				// previous read did time travel that already included this granule
				// FIXME: this will get more complicated with merges where this could potentially include partial
				// boundaries. For now with only splits we can skip the whole range
				continue;
			}
			ASSERT(readThrough == m->keyRange.begin);
			state Reference<GranuleMetadata> metadata = m;

			if (metadata->readable.canBeSet()) {
				wait(metadata->readable.getFuture());
			}
			if (metadata->cancelled.isSet()) {
				throw wrong_shard_server();
			}

			state KeyRange chunkRange;
			state GranuleFiles chunkFiles;

			if ((!metadata->files.snapshotFiles.empty() &&
			     metadata->files.snapshotFiles.front().version > req.readVersion) ||
			    (metadata->files.snapshotFiles.empty() && metadata->pendingSnapshotVersion > req.readVersion)) {
				// this is a time travel query, find previous granule
				if (metadata->historyLoaded.canBeSet()) {
					wait(metadata->historyLoaded.getFuture());
				}

				// FIXME: doesn't work once we add granule merging, could be multiple ranges and/or multiple parents
				Reference<GranuleHistoryEntry> cur = bwData->granuleHistory.rangeContaining(req.keyRange.begin).value();
				// FIXME: use skip pointers here
				while (cur.isValid() && req.readVersion < cur->startVersion) {
					cur = cur->parentGranule;
				}

				if (!cur.isValid()) {
					// this request predates blob data
					// FIXME: probably want a dedicated exception like blob_range_too_old or something instead
					throw transaction_too_old();
				}

				if (BW_REQUEST_DEBUG) {
					printf("[%s - %s) @ %lld time traveled back to %s [%s - %s) @ [%lld - %lld)\n",
					       req.keyRange.begin.printable().c_str(),
					       req.keyRange.end.printable().c_str(),
					       req.readVersion,
					       cur->granuleID.toString().c_str(),
					       cur->range.begin.printable().c_str(),
					       cur->range.end.printable().c_str(),
					       cur->startVersion,
					       cur->endVersion);
				}

				// lazily load files for old granule if not present
				chunkRange = cur->range;
				if (cur->files.isError() || !cur->files.isValid()) {
					cur->files = loadHistoryFiles(bwData, cur->granuleID);
				}

				GranuleFiles _f = wait(cur->files);
				chunkFiles = _f;

			} else {
				// this is an active granule query
				loop {
					Future<Void> waitForVersionFuture = waitForVersion(metadata, req.readVersion);
					if (waitForVersionFuture.isReady()) {
						// didn't yield, so no need to check rollback stuff
						break;
					}
					// rollback resets all of the version information, so we have to redo wait for version on rollback
					state int rollbackCount = metadata->rollbackCount.get();
					choose {
						when(wait(waitForVersionFuture)) {}
						when(wait(metadata->rollbackCount.onChange())) {}
						when(wait(metadata->cancelled.getFuture())) { throw wrong_shard_server(); }
					}

					if (rollbackCount == metadata->rollbackCount.get()) {
						break;
					} else if (BW_REQUEST_DEBUG) {
						printf("[%s - %s) @ %lld hit rollback, restarting waitForVersion\n",
						       req.keyRange.begin.printable().c_str(),
						       req.keyRange.end.printable().c_str(),
						       req.readVersion);
					}
				}
				chunkFiles = metadata->files;
				chunkRange = metadata->keyRange;
			}

			// granule is up to date, do read

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
			// because of granule history, we should always be able to find the desired snapshot version, and have
			// thrown transaction_too_old earlier if not possible.
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
				// we skipped one file at the end with a larger read version, this will actually contain our query
				// version, so add it back.
				i++;
			}
			// only include delta files after the snapshot file
			int j = i;
			while (j >= 0 && chunkFiles.deltaFiles[j].version > snapshotVersion) {
				j--;
			}
			j++;
			Version latestDeltaVersion = invalidVersion;
			while (j <= i) {
				BlobFileIndex deltaF = chunkFiles.deltaFiles[j];
				chunk.deltaFiles.emplace_back_deep(rep.arena, deltaF.filename, deltaF.offset, deltaF.length);
				bwData->stats.readReqDeltaBytesReturned += deltaF.length;
				latestDeltaVersion = deltaF.version;
				j++;
			}

			// new deltas (if version is larger than version of last delta file)
			// FIXME: do trivial key bounds here if key range is not fully contained in request key range

			if (req.readVersion > metadata->durableDeltaVersion.get()) {
				ASSERT(metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion);
				rep.arena.dependsOn(metadata->deltaArena);
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

ACTOR Future<Optional<GranuleHistory>> getLatestGranuleHistory(Transaction* tr, KeyRange range) {
	KeyRange historyRange = blobGranuleHistoryKeyRangeFor(range);
	RangeResult result = wait(tr->getRange(historyRange, 1, Snapshot::False, Reverse::True));
	ASSERT(result.size() <= 1);

	Optional<GranuleHistory> history;
	if (!result.empty()) {
		std::pair<KeyRange, Version> decodedKey = decodeBlobGranuleHistoryKey(result[0].key);
		ASSERT(range == decodedKey.first);
		history = GranuleHistory(range, decodedKey.second, decodeBlobGranuleHistoryValue(result[0].value));
	}
	return history;
}

ACTOR Future<GranuleStartState> openGranule(Reference<BlobWorkerData> bwData, AssignBlobRangeRequest req) {
	ASSERT(!req.continueAssignment);
	state Transaction tr(bwData->db);
	state Key lockKey = blobGranuleLockKeyFor(req.keyRange);

	if (BW_DEBUG) {
		printf("%s [%s - %s) opening\n",
		       bwData->id.toString().c_str(),
		       req.keyRange.begin.printable().c_str(),
		       req.keyRange.end.printable().c_str());
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
					// the only time history can be not present if a lock already exists is if it's a new granule and it
					// died before it could persist the initial snapshot from FDB
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

				// for the non-splitting cases, this doesn't need to be 100% accurate, it just needs to be
				// smaller than the next delta file write.
				info.changeFeedStartVersion = info.previousDurableVersion;
			} else {
				// else we are first, no need to check for owner conflict
				// FIXME: use actual 16 bytes of UID instead of converting it to 32 character string and then that
				// to bytes

				if (info.history.present()) {
					// if this granule is derived from a split or merge, this history entry is already present (written
					// by the blob manager)
					info.granuleID = info.history.get().value.granuleID;
				} else {
					// FIXME: could avoid max uid for granule ids here
					// if this granule is not derived from a split or merge, create the granule id here
					info.granuleID = deterministicRandom()->randomUniqueID();
				}
				wait(tr.registerChangeFeed(StringRef(info.granuleID.toString()), req.keyRange));
				info.doSnapshot = true;
				info.previousDurableVersion = invalidVersion;
			}

			tr.set(lockKey, blobGranuleLockValueFor(req.managerEpoch, req.managerSeqno, info.granuleID));
			wait(krmSetRange(&tr, blobGranuleMappingKeys.begin, req.keyRange, blobGranuleMappingValueFor(bwData->id)));

			// If anything in previousGranules, need to do the handoff logic and set ret.previousChangeFeedId, and
			// the previous durable version will come from the previous granules
			if (info.history.present() && info.history.get().value.parentGranules.size() > 0) {
				// TODO change this for merge
				ASSERT(info.history.get().value.parentGranules.size() == 1);
				state KeyRange parentGranuleRange = info.history.get().value.parentGranules[0].first;

				Optional<Value> parentGranuleLockValue = wait(tr.get(blobGranuleLockKeyFor(parentGranuleRange)));
				if (parentGranuleLockValue.present()) {
					std::tuple<int64_t, int64_t, UID> parentGranuleLock =
					    decodeBlobGranuleLockValue(parentGranuleLockValue.get());
					UID parentGranuleID = std::get<2>(parentGranuleLock);
					printf("  parent granule id %s\n", parentGranuleID.toString().c_str());

					info.parentGranule = std::pair(parentGranuleRange, parentGranuleID);

					state std::pair<BlobGranuleSplitState, Version> granuleSplitState =
					    std::pair(BlobGranuleSplitState::Started, invalidVersion);
					if (hasPrevOwner) {
						std::pair<BlobGranuleSplitState, Version> _gss =
						    wait(getGranuleSplitState(&tr, parentGranuleID, info.granuleID));
						granuleSplitState = _gss;
					}

					if (granuleSplitState.first == BlobGranuleSplitState::Assigned) {
						// was already assigned, use change feed start version
						ASSERT(granuleSplitState.second != invalidVersion);
						info.changeFeedStartVersion = granuleSplitState.second;
					} else if (granuleSplitState.first == BlobGranuleSplitState::Started) {
						wait(updateGranuleSplitState(&tr,
						                             info.parentGranule.get().first,
						                             info.parentGranule.get().second,
						                             info.granuleID,
						                             BlobGranuleSplitState::Assigned));
						// change feed was created as part of this transaction, changeFeedStartVersion will be
						// set later
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
// If a key range [A, B) was assigned to the worker at seqno S1, no part of the keyspace that intersects [A, B] may
// be re-assigned to the worker until the range has been revoked from this worker. This revoking can either happen
// by the blob manager willingly relinquishing the range, or by the blob manager reassigning it somewhere else. This
// means that if the worker gets an assignment for any range that intersects [A, B) at S3, there must have been a
// revoke message for [A, B) with seqno S3 where S1 < S2 < S3, that was delivered out of order. This means that if
// there are any intersecting but not fully overlapping ranges with a new range assignment, they had already been
// revoked. So the worker will mark them as revoked, but leave the sequence number as S1, so that when the actual
// revoke message comes in, it is a no-op, but updates the sequence number. Similarly, if a worker gets an assign
// message for any range that already has a higher sequence number, that range was either revoked, or revoked and
// then re-assigned. Either way, this assignment is no longer valid.

// Returns future to wait on to ensure prior work of other granules is done before responding to the manager with a
// successful assignment And if the change produced a new granule that needs to start doing work, returns the new
// granule so that the caller can start() it with the appropriate starting state.
ACTOR Future<bool> changeBlobRange(Reference<BlobWorkerData> bwData,
                                   KeyRange keyRange,
                                   int64_t epoch,
                                   int64_t seqno,
                                   bool active,
                                   bool disposeOnCleanup,
                                   bool selfReassign) {
	if (BW_DEBUG) {
		printf("%s range for [%s - %s): %s @ (%lld, %lld)\n",
		       selfReassign ? "Re-assigning" : "Changing",
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str(),
		       active ? "T" : "F",
		       epoch,
		       seqno);
	}

	// For each range that intersects this update:
	// If the identical range already exists at the same assignment sequence number and it is not a self-reassign,
	// this is a noop. Otherwise, this will consist of a series of ranges that are either older, or newer. For each
	// older range, cancel it if it is active. Insert the current range. Re-insert all newer ranges over the current
	// range.

	state std::vector<Future<Void>> futures;

	state std::vector<std::pair<KeyRange, GranuleRangeMetadata>> newerRanges;

	auto ranges = bwData->granuleMetadata.intersectingRanges(keyRange);
	bool alreadyAssigned = false;
	for (auto& r : ranges) {
		if (!active) {
			if (r.value().activeMetadata.isValid() && r.value().activeMetadata->cancelled.canBeSet()) {
				if (BW_DEBUG) {
					printf("Cancelling activeMetadata\n");
				}
				r.value().activeMetadata->cancelled.send(Void());
			}
		}
		bool thisAssignmentNewer = newerRangeAssignment(r.value(), epoch, seqno);
		if (r.value().lastEpoch == epoch && r.value().lastSeqno == seqno) {
			ASSERT(r.begin() == keyRange.begin);
			ASSERT(r.end() == keyRange.end);

			if (selfReassign) {
				thisAssignmentNewer = true;
			} else {
				printf("same assignment\n");
				// applied the same assignment twice, make idempotent
				if (r.value().activeMetadata.isValid()) {
					futures.push_back(success(r.value().assignFuture));
				}
				alreadyAssigned = true;
				break;
			}
		}

		if (r.value().activeMetadata.isValid() && thisAssignmentNewer) {
			// cancel actors for old range and clear reference
			if (BW_DEBUG) {
				printf("  [%s - %s): @ (%lld, %lld) (cancelling)\n",
				       r.begin().printable().c_str(),
				       r.end().printable().c_str(),
				       r.value().lastEpoch,
				       r.value().lastSeqno);
			}
			r.value().activeMetadata.clear();
		} else if (!thisAssignmentNewer) {
			// this assignment is outdated, re-insert it over the current range
			newerRanges.push_back(std::pair(r.range(), r.value()));
		}
	}

	if (alreadyAssigned) {
		wait(waitForAll(futures)); // already applied, nothing to do
		return false;
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

	printf("returning from changeblobrange");
	wait(waitForAll(futures));
	return true;
}

static bool resumeBlobRange(Reference<BlobWorkerData> bwData, KeyRange keyRange, int64_t epoch, int64_t seqno) {
	auto existingRange = bwData->granuleMetadata.rangeContaining(keyRange.begin);
	// if range boundaries don't match, or this (epoch, seqno) is old or the granule is inactive, ignore
	if (keyRange.begin != existingRange.begin() || keyRange.end != existingRange.end() ||
	    existingRange.value().lastEpoch > epoch ||
	    (existingRange.value().lastEpoch == epoch && existingRange.value().lastSeqno > seqno) ||
	    !existingRange.value().activeMetadata.isValid()) {

		if (BW_DEBUG) {
			printf("BW %s got out of date resume range for [%s - %s) @ (%lld, %lld). Currently  [%s - %s) @ (%lld, "
			       "%lld): %s\n",
			       bwData->id.toString().c_str(),
			       existingRange.begin().printable().c_str(),
			       existingRange.end().printable().c_str(),
			       existingRange.value().lastEpoch,
			       existingRange.value().lastSeqno,
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
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

ACTOR Future<Void> handleRangeAssign(Reference<BlobWorkerData> bwData,
                                     AssignBlobRangeRequest req,
                                     bool isSelfReassign) {
	try {
		if (req.continueAssignment) {
			resumeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno);
		} else {
			bool shouldStart = wait(
			    changeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno, true, false, isSelfReassign));

			if (shouldStart) {
				auto m = bwData->granuleMetadata.rangeContaining(req.keyRange.begin);
				ASSERT(m.begin() == req.keyRange.begin && m.end() == req.keyRange.end);
				wait(start(bwData, &m.value(), req));
			}
		}
		if (!isSelfReassign) {
			ASSERT(!req.reply.isSet());
			req.reply.send(AssignBlobRangeReply(true));
		}
		return Void();
	} catch (Error& e) {
		if (BW_DEBUG) {
			printf("AssignRange [%s - %s) got error %s\n",
			       req.keyRange.begin.printable().c_str(),
			       req.keyRange.end.printable().c_str(),
			       e.name());
		}

		if (!isSelfReassign) {
			if (canReplyWith(e)) {
				req.reply.sendError(e);
			}
		}

		throw;
	}
}

ACTOR Future<Void> handleRangeRevoke(Reference<BlobWorkerData> bwData, RevokeBlobRangeRequest req) {
	try {
		bool _shouldStart =
		    wait(changeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno, false, req.dispose, false));
		req.reply.send(AssignBlobRangeReply(true));
		return Void();
	} catch (Error& e) {
		// FIXME: retry on error if dispose fails?
		if (BW_DEBUG) {
			printf("RevokeRange [%s - %s) got error %s\n",
			       req.keyRange.begin.printable().c_str(),
			       req.keyRange.end.printable().c_str(),
			       e.name());
		}
		if (canReplyWith(e)) {
			req.reply.sendError(e);
		}
		throw;
	}
}

// Because change feeds send uncommitted data and explicit rollback messages, we speculatively buffer/write
// uncommitted data. This means we must ensure the data is actually committed before "committing" those writes in
// the blob granule. The simplest way to do this is to have the blob worker do a periodic GRV, which is guaranteed
// to be an earlier committed version.
ACTOR Future<Void> runCommitVersionChecks(Reference<BlobWorkerData> bwData) {
	state Transaction tr(bwData->db);
	loop {
		// only do grvs to get committed version if we need it to persist delta files
		while (bwData->pendingDeltaFileCommitChecks.get() == 0) {
			wait(bwData->pendingDeltaFileCommitChecks.onChange());
		}

		// batch potentially multiple delta files into one GRV, and also rate limit GRVs for this worker
		wait(delay(0.1)); // TODO KNOB?

		state int checksToResolve = bwData->pendingDeltaFileCommitChecks.get();

		tr.reset();
		try {
			Version readVersion = wait(tr.getReadVersion());

			ASSERT(readVersion >= bwData->knownCommittedVersion.get());
			if (readVersion > bwData->knownCommittedVersion.get()) {
				++bwData->knownCommittedCheckCount;
				bwData->knownCommittedVersion.set(readVersion);
				bwData->pendingDeltaFileCommitChecks.set(bwData->pendingDeltaFileCommitChecks.get() - checksToResolve);
			}
			++bwData->stats.commitVersionChecks;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
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
		if (g_network->isSimulated()) {
			if (BW_DEBUG) {
				printf("BW constructing simulated backup container\n");
			}
			self->bstore = BackupContainerFileSystem::openContainerFS("file://fdbblob/");
		} else {
			if (BW_DEBUG) {
				printf("BW constructing backup container from %s\n", SERVER_KNOBS->BG_URL.c_str());
			}
			self->bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL);
			if (BW_DEBUG) {
				printf("BW constructed backup container\n");
			}
		}

		// register the blob worker to the system keyspace
		wait(registerBlobWorker(self, bwInterf));
	} catch (Error& e) {
		if (BW_DEBUG) {
			printf("BW got backup container init error %s\n", e.name());
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
	self->addActor.send(runCommitVersionChecks(self));

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
						printf("Worker %s got new granule status endpoint\n", self->id.toString().c_str());
					}
					// TODO: pick a reasonable byte limit instead of just piggy-backing
					req.reply.setByteLimit(SERVER_KNOBS->RANGESTREAM_LIMIT_BYTES);
					self->currentManagerStatusStream.set(req.reply);
				}
			}
			when(AssignBlobRangeRequest _req = waitNext(bwInterf.assignBlobRangeRequest.getFuture())) {
				++self->stats.rangeAssignmentRequests;
				--self->stats.numRangesAssigned;
				state AssignBlobRangeRequest assignReq = _req;
				if (BW_DEBUG) {
					printf("Worker %s assigned range [%s - %s) @ (%lld, %lld):\n  continue=%s\n",
					       self->id.toString().c_str(),
					       assignReq.keyRange.begin.printable().c_str(),
					       assignReq.keyRange.end.printable().c_str(),
					       assignReq.managerEpoch,
					       assignReq.managerSeqno,
					       assignReq.continueAssignment ? "T" : "F");
				}

				if (self->managerEpochOk(assignReq.managerEpoch)) {
					self->addActor.send(handleRangeAssign(self, assignReq, false));
				} else {
					assignReq.reply.send(AssignBlobRangeReply(false));
				}
			}
			when(RevokeBlobRangeRequest _req = waitNext(bwInterf.revokeBlobRangeRequest.getFuture())) {
				state RevokeBlobRangeRequest revokeReq = _req;
				--self->stats.numRangesAssigned;
				if (BW_DEBUG) {
					printf("Worker %s revoked range [%s - %s) @ (%lld, %lld):\n  dispose=%s\n",
					       self->id.toString().c_str(),
					       revokeReq.keyRange.begin.printable().c_str(),
					       revokeReq.keyRange.end.printable().c_str(),
					       revokeReq.managerEpoch,
					       revokeReq.managerSeqno,
					       revokeReq.dispose ? "T" : "F");
				}

				if (self->managerEpochOk(revokeReq.managerEpoch)) {
					self->addActor.send(handleRangeRevoke(self, revokeReq));
				} else {
					revokeReq.reply.send(AssignBlobRangeReply(false));
				}
			}
			when(AssignBlobRangeRequest granuleToReassign = waitNext(self->granuleUpdateErrors.getFuture())) {
				self->addActor.send(handleRangeAssign(self, granuleToReassign, true));
			}
			when(HaltBlobWorkerRequest req = waitNext(bwInterf.haltBlobWorker.getFuture())) {
				req.reply.send(Void());
				if (self->managerEpochOk(req.managerEpoch)) {
					TraceEvent("BlobWorkerHalted", bwInterf.id()).detail("ReqID", req.requesterID);
					printf("BW %s was halted\n", bwInterf.id().toString().c_str());
					break;
				}
			}
			when(wait(collection)) {
				TraceEvent("BlobWorkerActorCollectionError");
				ASSERT(false);
				throw internal_error();
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
