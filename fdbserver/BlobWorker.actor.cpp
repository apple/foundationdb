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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/BlobWorkerCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/BlobWorker.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/WaitFailure.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // has to be last include
#include "flow/flow.h"

#define BW_DEBUG true
#define BW_REQUEST_DEBUG false

// FIXME: change all BlobWorkerData* to Reference<BlobWorkerData> to avoid segfaults if core loop gets error

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

// TODO needs better name, it's basically just "granule starting state"
struct GranuleChangeFeedInfo {
	UID changeFeedId;
	Version changeFeedStartVersion;
	Version previousDurableVersion;
	Optional<UID> prevChangeFeedId;
	bool doSnapshot;
	Optional<KeyRange> granuleSplitFrom;
	Optional<GranuleFiles> blobFilesToSnapshot;
	Optional<GranuleFiles> existingFiles;
};

// FIXME: the circular dependencies here are getting kind of gross
struct GranuleMetadata;
struct BlobWorkerData;
ACTOR Future<GranuleChangeFeedInfo> persistAssignWorkerRange(BlobWorkerData* bwData, AssignBlobRangeRequest req);
ACTOR Future<Void> blobGranuleUpdateFiles(BlobWorkerData* bwData, Reference<GranuleMetadata> metadata);

// for a range that is active
struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	KeyRange keyRange;

	GranuleFiles files;
	GranuleDeltas currentDeltas;
	// TODO get rid of this and do Reference<Standalone<GranuleDeltas>>?
	Arena deltaArena;

	uint64_t bytesInNewDeltaFiles = 0;
	uint64_t bufferedDeltaBytes = 0;

	NotifiedVersion bufferedDeltaVersion;
	Version pendingDeltaVersion = 0;
	NotifiedVersion durableDeltaVersion;
	NotifiedVersion durableSnapshotVersion;
	Version pendingSnapshotVersion = 0;

	AsyncVar<int> rollbackCount;

	int64_t originalEpoch;
	int64_t originalSeqno;
	int64_t continueEpoch;
	int64_t continueSeqno;

	Future<GranuleChangeFeedInfo> assignFuture;
	Future<Void> fileUpdaterFuture;
	Promise<Void> resumeSnapshot;
	Promise<Void> cancelled;
	Promise<Void> readable;

	AssignBlobRangeRequest originalReq;

	Future<Void> start(BlobWorkerData* bwData, AssignBlobRangeRequest req) {
		originalReq = req;
		assignFuture = persistAssignWorkerRange(bwData, req);
		fileUpdaterFuture = blobGranuleUpdateFiles(bwData, Reference<GranuleMetadata>::addRef(this));

		return success(assignFuture);
	}

	void resume() {
		ASSERT(resumeSnapshot.canBeSet());
		resumeSnapshot.send(Void());
	}

	// FIXME: right now there is a dependency because this contains both the actual file/delta data as well as the
	// metadata (worker futures), so removing this reference from the map doesn't actually cancel the workers. It'd be
	// better to have this in 2 separate objects, where the granule metadata map has the futures, but the read
	// queries/file updater/range feed only copy the reference to the file/delta data.
	Future<Void> cancel(bool dispose) {
		if (cancelled.canBeSet()) {
			// Could have been cancelled already by rollback or error in BGUpdateFiles
			cancelled.send(Void());
		}
		assignFuture.cancel();
		fileUpdaterFuture.cancel();

		if (dispose) {
			// FIXME: implement dispose!
			return delay(0.1);
		}
		return Future<Void>(Void());
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

	BlobWorkerStats stats;

	LocalityData locality;
	int64_t currentManagerEpoch = -1;

	ReplyPromiseStream<GranuleStatusReply> currentManagerStatusStream;

	// FIXME: refactor out the parts of this that are just for interacting with blob stores from the backup business
	// logic
	Reference<BackupContainerFileSystem> bstore;
	KeyRangeMap<GranuleRangeMetadata> granuleMetadata;

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
	    "Checking granule lock: \n  mine: (%lld, %lld)\n  owner: (%lld, %lld)\n", epoch, seqno, ownerEpoch, ownerSeqno);
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

// TODO this is duplicated with blob manager: fix?
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
	std::tuple<int64_t, int64_t, UID> currentOwner = decodeBlobGranuleLockValue(lockValue.get());
	checkGranuleLock(epoch, seqno, std::get<0>(currentOwner), std::get<1>(currentOwner));

	// if we still own the lock, add a conflict range in case anybody else takes it over while we add this file
	tr->addReadConflictRange(singleKeyRange(lockKey));

	return Void();
}

ACTOR Future<GranuleFiles> loadPreviousFiles(Transaction* tr, KeyRange keyRange) {
	// read everything from previous granule of snapshot and delta files
	Tuple prevFilesStartTuple;
	prevFilesStartTuple.append(keyRange.begin).append(keyRange.end);
	Tuple prevFilesEndTuple;
	prevFilesEndTuple.append(keyRange.begin).append(keyAfter(keyRange.end));
	Key prevFilesStartKey = prevFilesStartTuple.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin);
	Key prevFilesEndKey = prevFilesEndTuple.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin);

	state KeyRange currentRange = KeyRangeRef(prevFilesStartKey, prevFilesEndKey);

	state GranuleFiles files;

	loop {
		int lim = BUGGIFY ? 2 : 1000;
		RangeResult res = wait(tr->getRange(currentRange, lim));
		for (auto& it : res) {
			Tuple fileKey = Tuple::unpack(it.key.removePrefix(blobGranuleFileKeys.begin));
			Tuple fileValue = Tuple::unpack(it.value);

			ASSERT(fileKey.size() == 4);
			ASSERT(fileValue.size() == 3);

			ASSERT(fileKey.getString(0) == keyRange.begin);
			ASSERT(fileKey.getString(1) == keyRange.end);

			std::string fileType = fileKey.getString(2).toString();
			ASSERT(fileType == LiteralStringRef("S") || fileType == LiteralStringRef("D"));

			BlobFileIndex idx(
			    fileKey.getInt(3), fileValue.getString(0).toString(), fileValue.getInt(1), fileValue.getInt(2));
			if (fileType == LiteralStringRef("S")) {
				ASSERT(files.snapshotFiles.empty() || files.snapshotFiles.back().version < idx.version);
				files.snapshotFiles.push_back(idx);
			} else {
				ASSERT(files.deltaFiles.empty() || files.deltaFiles.back().version < idx.version);
				files.deltaFiles.push_back(idx);
			}
		}
		if (res.more) {
			currentRange = KeyRangeRef(keyAfter(res.back().key), currentRange.end);
		} else {
			break;
		}
	}
	if (BW_DEBUG) {
		printf("Loaded %d snapshot and %d delta previous files for [%s - %s)\n",
		       files.snapshotFiles.size(),
		       files.deltaFiles.size(),
		       keyRange.begin.printable().c_str(),
		       keyRange.end.printable().c_str());
	}
	return files;
}

// To cleanup of the old change feed for the old granule range, all new sub-granules split from the old range must
// update shared state to coordinate when it is safe to clean up the old change feed.
//  his goes through 3 phases for each new sub-granule:
//  1. Starting - the blob manager writes all sub-granules with this state as a durable intent to split the range
//  2. Assigned - a worker that is assigned a sub-granule updates that granule's state here. This means that the worker
//     has started a new change feed for the new sub-granule, but still needs to consume from the old change feed.
//  3. Done - the worker that is assigned this sub-granule has persisted all of the data from its part of the old change
//     feed in delta files. From this granule's perspective, it is safe to clean up the old change feed.

// Once all sub-granules have reached step 2 (Assigned), the change feed can be safely "stopped" - it needs to continue
// to serve the mutations it has seen so far, but will not need any new mutations after this version.
// The last sub-granule to reach this step is responsible for commiting the change feed stop as part of its
// transaction. Because this change feed stops commits in the same transaction as the worker's new change feed start,
// it is guaranteed that no versions are missed between the old and new change feed.
//
// Once all sub-granules have reached step 3 (Done), the change feed can be safely destroyed, as all of the mutations in
// the old change feed are guaranteed to be persisted in delta files. The last sub-granule to reach this step is
// responsible for committing the change feed destroy, and for cleaning up the split state for all sub-granules as part
// of its transaction.

ACTOR Future<Void> updateGranuleSplitState(Transaction* tr,
                                           KeyRange previousGranule,
                                           KeyRange currentGranule,
                                           UID prevChangeFeedId,
                                           BlobGranuleSplitState newState) {
	// read all splitting state for previousGranule. If it doesn't exist, newState must == DONE
	Tuple splitStateStartTuple;
	splitStateStartTuple.append(previousGranule.begin).append(previousGranule.end);
	Tuple splitStateEndTuple;
	splitStateEndTuple.append(previousGranule.begin).append(keyAfter(previousGranule.end));

	Key splitStateStartKey = splitStateStartTuple.getDataAsStandalone().withPrefix(blobGranuleSplitKeys.begin);
	Key splitStateEndKey = splitStateEndTuple.getDataAsStandalone().withPrefix(blobGranuleSplitKeys.begin);

	state KeyRange currentRange = KeyRangeRef(splitStateStartKey, splitStateEndKey);

	RangeResult totalState = wait(tr->getRange(currentRange, 100));
	// TODO is this explicit conflit range necessary with the above read?
	tr->addWriteConflictRange(currentRange);
	ASSERT(!totalState.more);

	if (totalState.empty()) {
		ASSERT(newState == BlobGranuleSplitState::Done);
		if (BW_DEBUG) {
			printf("Found empty split state for previous granule [%s - %s)\n",
			       previousGranule.begin.printable().c_str(),
			       previousGranule.end.printable().c_str());
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
		Tuple key = Tuple::unpack(it.key.removePrefix(blobGranuleSplitKeys.begin));
		ASSERT(key.getString(0) == previousGranule.begin);
		ASSERT(key.getString(1) == previousGranule.end);

		BlobGranuleSplitState st = decodeBlobGranuleSplitValue(it.value).first;
		ASSERT(st != BlobGranuleSplitState::Unknown);
		if (st == BlobGranuleSplitState::Started) {
			totalStarted++;
		} else if (st == BlobGranuleSplitState::Done) {
			totalDone++;
		}
		if (key.getString(2) == currentGranule.begin) {
			ASSERT(currentState == BlobGranuleSplitState::Unknown);
			currentState = st;
		}
	}

	ASSERT(currentState != BlobGranuleSplitState::Unknown);

	if (currentState < newState) {
		if (BW_DEBUG) {
			printf("Updating granule [%s - %s) split state from [%s - %s) %d -> %d\n",
			       currentGranule.begin.printable().c_str(),
			       currentGranule.end.printable().c_str(),
			       previousGranule.begin.printable().c_str(),
			       previousGranule.end.printable().c_str(),
			       currentState,
			       newState);
		}

		Tuple myStateTuple;
		myStateTuple.append(previousGranule.begin).append(previousGranule.end).append(currentGranule.begin);
		Key myStateKey = myStateTuple.getDataAsStandalone().withPrefix(blobGranuleSplitKeys.begin);
		if (newState == BlobGranuleSplitState::Done && currentState == BlobGranuleSplitState::Assigned &&
		    totalDone == total - 1) {
			// we are the last one to change from Assigned -> Done, so everything can be cleaned up for the old change
			// feed and splitting state
			if (BW_DEBUG) {
				printf("[%s - %s) destroying old change feed %s and granule lock + split state for [%s - %s)\n",
				       currentGranule.begin.printable().c_str(),
				       currentGranule.end.printable().c_str(),
				       prevChangeFeedId.toString().c_str(),
				       previousGranule.begin.printable().c_str(),
				       previousGranule.end.printable().c_str());
			}
			Key oldGranuleLockKey = granuleLockKey(previousGranule);
			tr->destroyChangeFeed(KeyRef(prevChangeFeedId.toString()));
			tr->clear(singleKeyRange(oldGranuleLockKey));
			tr->clear(currentRange);
		} else {
			if (newState == BlobGranuleSplitState::Assigned && currentState == BlobGranuleSplitState::Started &&
			    totalStarted == 1) {
				if (BW_DEBUG) {
					printf("[%s - %s) WOULD BE stopping old change feed %s for [%s - %s)\n",
					       currentGranule.begin.printable().c_str(),
					       currentGranule.end.printable().c_str(),
					       prevChangeFeedId.toString().c_str(),
					       previousGranule.begin.printable().c_str(),
					       previousGranule.end.printable().c_str());
				}
				// FIXME: enable once implemented
				// tr.stopChangeFeed(KeyRef(prevChangeFeedId.toString()));
			}
			// TODO also add versionstamp
			tr->atomicOp(myStateKey, blobGranuleSplitValueFor(newState), MutationRef::SetVersionstampedValue);
		}
	} else if (BW_DEBUG) {
		printf("Ignoring granule [%s - %s) split state from [%s - %s) %d -> %d\n",
		       currentGranule.begin.printable().c_str(),
		       currentGranule.end.printable().c_str(),
		       previousGranule.begin.printable().c_str(),
		       previousGranule.end.printable().c_str(),
		       currentState,
		       newState);
	}

	return Void();
}

// returns the split state for a given granule on granule reassignment
ACTOR Future<std::pair<BlobGranuleSplitState, Version>> getGranuleSplitState(Transaction* tr,
                                                                             KeyRange previousGranule,
                                                                             KeyRange currentGranule) {
	Tuple myStateTuple;
	myStateTuple.append(previousGranule.begin).append(previousGranule.end).append(currentGranule.begin);
	Key myStateKey = myStateTuple.getDataAsStandalone().withPrefix(blobGranuleSplitKeys.begin);

	Optional<Value> st = wait(tr->get(myStateKey));
	if (!st.present()) {
		// must have been that all granules reached done and state was cleaned up
		return std::pair(BlobGranuleSplitState::Done, invalidVersion);
	}
	return decodeBlobGranuleSplitValue(st.get());
}

static Value getFileValue(std::string fname, int64_t offset, int64_t length) {
	Tuple fileValue;
	fileValue.append(fname).append(offset).append(length);
	return fileValue.getDataAsStandalone();
}

// writeDelta file writes speculatively in the common case to optimize throughput. It creates the s3 object even though
// the data in it may not yet be committed, and even though previous delta fiels with lower versioned data may still be
// in flight. The synchronization happens after the s3 file is written, but before we update the FDB index of what files
// exist. Before updating FDB, we ensure the version is committed and all previous delta files have updated FDB.
ACTOR Future<BlobFileIndex> writeDeltaFile(BlobWorkerData* bwData,
                                           KeyRange keyRange,
                                           int64_t epoch,
                                           int64_t seqno,
                                           Arena deltaArena,
                                           GranuleDeltas deltasToWrite,
                                           Version currentDeltaVersion,
                                           Future<BlobFileIndex> previousDeltaFileFuture,
                                           Optional<KeyRange> oldChangeFeedDataComplete,
                                           Optional<UID> oldChangeFeedId) {
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
				Tuple deltaFileKey;
				deltaFileKey.append(keyRange.begin).append(keyRange.end);
				deltaFileKey.append(LiteralStringRef("D")).append(currentDeltaVersion);

				Key dfKey = deltaFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin);
				tr->set(dfKey, getFileValue(fname, 0, serialized.size()));

				// FIXME: if previous granule present and delta file version >= previous change feed version, update the
				// state here
				if (oldChangeFeedDataComplete.present()) {
					ASSERT(oldChangeFeedId.present());
					wait(updateGranuleSplitState(&tr->getTransaction(),
					                             oldChangeFeedDataComplete.get(),
					                             keyRange,
					                             oldChangeFeedId.get(),
					                             BlobGranuleSplitState::Done));
				}

				wait(tr->commit());
				if (BW_DEBUG) {
					printf("Granule [%s - %s) updated fdb with delta file %s of size %d at version %lld, cv=%lld\n",
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
				wait(tr->onError(e));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}

		// FIXME: only delete if key doesn't exist
		if (BW_DEBUG) {
			printf("deleting s3 delta file %s after error %s\n", fname.c_str(), e.name());
		}
		state Error eState = e;
		++bwData->stats.s3DeleteReqs;
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
	// TODO add conflict range for writes?
	state Tuple snapshotFileKey;
	snapshotFileKey.append(keyRange.begin).append(keyRange.end);
	snapshotFileKey.append(LiteralStringRef("S")).append(version);

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
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}

		// FIXME: only delete if key doesn't exist
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
			    bwData, metadata->keyRange, metadata->originalEpoch, metadata->originalSeqno, readVersion, rowsStream);

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

// files might not be the current set of files in metadata, in the case of doing the initial snapshot of a granule.
ACTOR Future<BlobFileIndex> compactFromBlob(BlobWorkerData* bwData,
                                            Reference<GranuleMetadata> metadata,
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
	chunk.snapshotFile = BlobFilenameRef(filenameArena, snapshotF.filename, snapshotF.offset, snapshotF.length);
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
			state Future<BlobFileIndex> snapshotWriter = writeSnapshot(
			    bwData, metadata->keyRange, metadata->originalEpoch, metadata->originalSeqno, version, rowsStream);
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

ACTOR Future<Void> handleCompletedDeltaFile(BlobWorkerData* bwData,
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

		// delete all deltas in rollback range, but we can optimize here to just skip the uncommitted mutations directly
		// and immediately pop the rollback out of inProgress
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
ACTOR Future<Void> blobGranuleUpdateFiles(BlobWorkerData* bwData, Reference<GranuleMetadata> metadata) {
	state PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> oldChangeFeedStream;
	state PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> changeFeedStream;
	state Future<BlobFileIndex> inFlightBlobSnapshot;
	state std::deque<InFlightDeltaFile> inFlightDeltaFiles;
	state Future<Void> oldChangeFeedFuture;
	state Future<Void> changeFeedFuture;
	state GranuleChangeFeedInfo changeFeedInfo;
	state bool readOldChangeFeed;
	state bool lastFromOldChangeFeed = false;
	state Optional<KeyRange> oldChangeFeedDataComplete;
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
		GranuleChangeFeedInfo _info = wait(metadata->assignFuture);
		changeFeedInfo = _info;

		wait(delay(0, TaskPriority::BlobWorkerUpdateStorage));

		cfKey = StringRef(changeFeedInfo.changeFeedId.toString());
		if (changeFeedInfo.prevChangeFeedId.present()) {
			oldCFKey = StringRef(changeFeedInfo.prevChangeFeedId.get().toString());
		}

		if (BW_DEBUG) {
			printf("Granule File Updater Starting for [%s - %s):\n",
			       metadata->keyRange.begin.printable().c_str(),
			       metadata->keyRange.end.printable().c_str());
			printf("  CFID: %s\n", changeFeedInfo.changeFeedId.toString().c_str());
			printf("  CF Start Version: %lld\n", changeFeedInfo.changeFeedStartVersion);
			printf("  Previous Durable Version: %lld\n", changeFeedInfo.previousDurableVersion);
			printf("  doSnapshot=%s\n", changeFeedInfo.doSnapshot ? "T" : "F");
			printf("  Prev CFID: %s\n",
			       changeFeedInfo.prevChangeFeedId.present() ? changeFeedInfo.prevChangeFeedId.get().toString().c_str()
			                                                 : "");
			printf("  granuleSplitFrom=%s\n", changeFeedInfo.granuleSplitFrom.present() ? "T" : "F");
			printf("  blobFilesToSnapshot=%s\n", changeFeedInfo.blobFilesToSnapshot.present() ? "T" : "F");
		}

		// FIXME: handle reassigns by not doing a snapshot!!
		state Version startVersion;
		state BlobFileIndex newSnapshotFile;

		inFlightBlobSnapshot = Future<BlobFileIndex>(); // not valid!

		// if this is a reassign, calculate how close to a snapshot the previous owner was
		if (changeFeedInfo.existingFiles.present()) {
			GranuleFiles files = changeFeedInfo.existingFiles.get();
			if (!files.snapshotFiles.empty() && !files.deltaFiles.empty()) {
				Version snapshotVersion = files.snapshotFiles.back().version;
				for (int i = files.deltaFiles.size() - 1; i >= 0; i--) {
					if (files.deltaFiles[i].version > snapshotVersion) {
						metadata->bytesInNewDeltaFiles += files.deltaFiles[i].length;
					}
				}
			}
			metadata->files = changeFeedInfo.existingFiles.get();
			snapshotEligible = true;
		}

		// FIXME: not true for reassigns
		if (!changeFeedInfo.doSnapshot) {
			startVersion = changeFeedInfo.previousDurableVersion;
			ASSERT(!metadata->files.snapshotFiles.empty());
			metadata->pendingSnapshotVersion = metadata->files.snapshotFiles.back().version;
			metadata->durableSnapshotVersion.set(metadata->pendingSnapshotVersion);
		} else {
			if (changeFeedInfo.blobFilesToSnapshot.present()) {
				inFlightBlobSnapshot = compactFromBlob(bwData, metadata, changeFeedInfo.blobFilesToSnapshot.get());
				startVersion = changeFeedInfo.previousDurableVersion;
				metadata->durableSnapshotVersion.set(
				    changeFeedInfo.blobFilesToSnapshot.get().snapshotFiles.back().version);
			} else {
				ASSERT(changeFeedInfo.previousDurableVersion == invalidVersion);
				BlobFileIndex fromFDB = wait(dumpInitialSnapshotFromFDB(bwData, metadata));
				newSnapshotFile = fromFDB;
				ASSERT(changeFeedInfo.changeFeedStartVersion <= fromFDB.version);
				startVersion = newSnapshotFile.version;
				metadata->files.snapshotFiles.push_back(newSnapshotFile);
				metadata->durableSnapshotVersion.set(startVersion);

				wait(yield(TaskPriority::BlobWorkerUpdateStorage));
			}
			metadata->pendingSnapshotVersion = startVersion;
		}

		metadata->durableDeltaVersion.set(startVersion);
		metadata->pendingDeltaVersion = startVersion;
		metadata->bufferedDeltaVersion.set(startVersion);

		ASSERT(metadata->readable.canBeSet());
		metadata->readable.send(Void());

		if (changeFeedInfo.prevChangeFeedId.present()) {
			// FIXME: once we have empty versions, only include up to changeFeedInfo.changeFeedStartVersion in the read
			// stream. Then we can just stop the old stream when we get end_of_stream from this and not handle the
			// mutation version truncation stuff

			// FIXME: filtering on key range != change feed range doesn't work
			ASSERT(changeFeedInfo.granuleSplitFrom.present());
			readOldChangeFeed = true;
			oldChangeFeedFuture =
			    bwData->db->getChangeFeedStream(oldChangeFeedStream,
			                                    oldCFKey.get(),
			                                    startVersion + 1,
			                                    MAX_VERSION,
			                                    changeFeedInfo.granuleSplitFrom.get() /*metadata->keyRange*/);
		} else {
			readOldChangeFeed = false;
			changeFeedFuture = bwData->db->getChangeFeedStream(
			    changeFeedStream, cfKey, startVersion + 1, MAX_VERSION, metadata->keyRange);
		}

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
						                              changeFeedInfo.changeFeedStartVersion,
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
				        metadata->keyRange, &oldMutations, &mutations, changeFeedInfo.changeFeedStartVersion)) {
					// if old change feed has caught up with where new one would start, finish last one and start new
					// one

					Key cfKey = StringRef(changeFeedInfo.changeFeedId.toString());
					changeFeedFuture = bwData->db->getChangeFeedStream(changeFeedStream,
					                                                   cfKey,
					                                                   changeFeedInfo.changeFeedStartVersion,
					                                                   MAX_VERSION,
					                                                   metadata->keyRange);
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
				ASSERT(deltas.version >= metadata->bufferedDeltaVersion.get());
				// Write a new delta file IF we have enough bytes, and we have all of the previous version's stuff
				// there to ensure no versions span multiple delta files. Check this by ensuring the version of this new
				// delta is larger than the previous largest seen version
				if (metadata->bufferedDeltaBytes >= SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES &&
				    deltas.version > metadata->bufferedDeltaVersion.get()) {
					if (BW_DEBUG) {
						printf("Granule [%s - %s) flushing delta file after %d bytes @ %lld %lld%s\n",
						       metadata->keyRange.begin.printable().c_str(),
						       metadata->keyRange.end.printable().c_str(),
						       metadata->bufferedDeltaBytes,
						       metadata->bufferedDeltaVersion.get(),
						       deltas.version,
						       oldChangeFeedDataComplete.present() ? ". Finalizing " : "");
					}
					TraceEvent("BlobGranuleDeltaFile", bwData->id)
					    .detail("Granule", metadata->keyRange)
					    .detail("Version", metadata->bufferedDeltaVersion.get());

					// sanity check for version order
					ASSERT(metadata->bufferedDeltaVersion.get() >= metadata->currentDeltas.back().version);
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
					                                                metadata->originalEpoch,
					                                                metadata->originalSeqno,
					                                                metadata->deltaArena,
					                                                metadata->currentDeltas,
					                                                metadata->bufferedDeltaVersion.get(),
					                                                previousDeltaFileFuture,
					                                                oldChangeFeedDataComplete,
					                                                changeFeedInfo.prevChangeFeedId);
					inFlightDeltaFiles.push_back(InFlightDeltaFile(
					    dfFuture, metadata->bufferedDeltaVersion.get(), metadata->bufferedDeltaBytes));

					oldChangeFeedDataComplete.reset();
					// add new pending delta file
					if (metadata->pendingDeltaVersion >= metadata->bufferedDeltaVersion.get()) {
						printf("%lld >= %lld\n", metadata->pendingDeltaVersion, metadata->bufferedDeltaVersion.get());
					}
					ASSERT(metadata->pendingDeltaVersion < metadata->bufferedDeltaVersion.get());
					metadata->pendingDeltaVersion = metadata->bufferedDeltaVersion.get();
					metadata->bytesInNewDeltaFiles += metadata->bufferedDeltaBytes;

					bwData->stats.mutationBytesBuffered -= metadata->bufferedDeltaBytes;

					// reset current deltas
					metadata->deltaArena = Arena();
					metadata->currentDeltas = GranuleDeltas();
					metadata->bufferedDeltaBytes = 0;

					// if we just wrote a delta file, check if we need to compact here.
					// exhaust old change feed before compacting - otherwise we could end up with an endlessly growing
					// list of previous change feeds in the worst case.
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
						                              changeFeedInfo.changeFeedStartVersion,
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
						bwData->currentManagerStatusStream.send(
						    GranuleStatusReply(metadata->keyRange, true, statusEpoch, statusSeqno));

						Optional<Void> result = wait(timeout(metadata->resumeSnapshot.getFuture(), 1.0));
						if (result.present()) {
							break;
						}
						// FIXME: re-trigger this loop if blob manager status stream changes
						if (BW_DEBUG) {
							printf("Granule [%s - %s)\n, hasn't heard back from BM, re-sending status\n",
							       metadata->keyRange.begin.printable().c_str(),
							       metadata->keyRange.end.printable().c_str());
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
					// BlobFileIndex newSnapshotFile = wait(compactFromBlob(bwData, metadata, metadata->files));

					// Have to copy files object so that adding to it as we start writing new delta files in
					// parallel doesn't conflict. We could also pass the snapshot version and ignore any snapshot
					// files >= version and any delta files > version, but that's more complicated
					inFlightBlobSnapshot = compactFromBlob(bwData, metadata, metadata->files);
					metadata->pendingSnapshotVersion = metadata->durableDeltaVersion.get();

					// reset metadata
					metadata->bytesInNewDeltaFiles = 0;
				} else if (snapshotEligible &&
				           metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
					// if we're in the old change feed case and can't snapshot but we have enough data to, don't queue
					// too many delta files in parallel
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
						                              changeFeedInfo.changeFeedStartVersion,
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
							// explicitly check last delta in currentDeltas because metadata-bufferedDeltaVersion
							// includes empties
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
									    changeFeedInfo.granuleSplitFrom.get() /*metadata->keyRange*/);
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
				} else {
					ASSERT(metadata->bufferedDeltaVersion.get() <= deltas.version);
					if (metadata->bufferedDeltaVersion.get() < deltas.version) {
						metadata->bufferedDeltaVersion.set(deltas.version);
					}
				}
			}
			if (lastFromOldChangeFeed && !justDidRollback) {
				readOldChangeFeed = false;
				lastFromOldChangeFeed = false;
				// set this so next delta file write updates granule split metadata to done
				ASSERT(changeFeedInfo.granuleSplitFrom.present());
				oldChangeFeedDataComplete = changeFeedInfo.granuleSplitFrom;
				if (BW_DEBUG) {
					printf("Granule [%s - %s) switching to new change feed %s @ %lld\n",
					       metadata->keyRange.begin.printable().c_str(),
					       metadata->keyRange.end.printable().c_str(),
					       changeFeedInfo.changeFeedId.toString().c_str(),
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
				// explicitly cancel all outstanding write futures BEFORE updating promise stream, to ensure they can't
				// update files after the re-assigned granule acquires the lock
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

ACTOR Future<Void> waitForVersionActor(Reference<GranuleMetadata> metadata, Version v) {
	if (metadata->readable.canBeSet()) {
		wait(metadata->readable.getFuture());
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

static Future<Void> waitForVersion(Reference<GranuleMetadata> metadata, Version v) {
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

	if (metadata->readable.isSet() && v <= metadata->bufferedDeltaVersion.get() &&
	    (v <= metadata->durableDeltaVersion.get() ||
	     metadata->durableDeltaVersion.get() == metadata->pendingDeltaVersion) &&
	    (v <= metadata->durableSnapshotVersion.get() ||
	     metadata->durableSnapshotVersion.get() == metadata->pendingSnapshotVersion)) {
		return Void();
	}

	return waitForVersionActor(metadata, v);
}

ACTOR Future<Void> handleBlobGranuleFileRequest(BlobWorkerData* bwData, BlobGranuleFileRequest req) {
	try {
		// TODO REMOVE in api V2
		ASSERT(req.beginVersion == 0);
		state BlobGranuleFileReply rep;
		state vector<Reference<GranuleMetadata>> granules;

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
		for (auto m : granules) {
			state Reference<GranuleMetadata> metadata = m;
			// try to check version_too_old, cancelled, waitForVersion without yielding first
			if (metadata->readable.isSet() &&
			    ((!metadata->files.snapshotFiles.empty() &&
			      metadata->files.snapshotFiles.front().version > req.readVersion) ||
			     (metadata->files.snapshotFiles.empty() && metadata->pendingSnapshotVersion > req.readVersion))) {
				if (BW_REQUEST_DEBUG) {
					printf("Oldest snapshot file for [%s - %s) is @ %lld, later than request version %lld\n",
					       req.keyRange.begin.printable().c_str(),
					       req.keyRange.end.printable().c_str(),
					       metadata->files.snapshotFiles.size() == 0 ? 0 : metadata->files.snapshotFiles[0].version,
					       req.readVersion);
				}
				throw transaction_too_old();
			}
			if (metadata->cancelled.isSet()) {
				throw wrong_shard_server();
			}

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

			// granule is up to date, do read

			BlobGranuleChunkRef chunk;
			// TODO change in V2
			chunk.includedVersion = req.readVersion;
			chunk.keyRange = KeyRangeRef(StringRef(rep.arena, metadata->keyRange.begin),
			                             StringRef(rep.arena, metadata->keyRange.end));

			// handle snapshot files
			// TODO refactor the "find snapshot file" logic to GranuleFiles?
			// FIXME: binary search instead of linear search, especially when file count is large
			int i = metadata->files.snapshotFiles.size() - 1;
			while (i >= 0 && metadata->files.snapshotFiles[i].version > req.readVersion) {
				i--;
			}
			// if version is older than oldest snapshot file (or no snapshot files), throw too old
			// FIXME: probably want a dedicated exception like blob_range_too_old or something instead
			if (i < 0) {
				if (BW_REQUEST_DEBUG) {
					printf("Req [%s - %s) @ %lld Got too old after initial check.\n",
					       req.keyRange.begin.printable().c_str(),
					       req.keyRange.end.printable().c_str(),
					       req.readVersion);
				}
				throw transaction_too_old();
			}

			BlobFileIndex snapshotF = metadata->files.snapshotFiles[i];
			chunk.snapshotFile = BlobFilenameRef(rep.arena, snapshotF.filename, snapshotF.offset, snapshotF.length);
			Version snapshotVersion = metadata->files.snapshotFiles[i].version;

			// handle delta files
			// cast this to an int so i going to -1 still compares properly
			int lastDeltaFileIdx = metadata->files.deltaFiles.size() - 1;
			i = lastDeltaFileIdx;
			// skip delta files that are too new
			while (i >= 0 && metadata->files.deltaFiles[i].version > req.readVersion) {
				i--;
			}
			if (i < lastDeltaFileIdx) {
				// we skipped one file at the end with a larger read version, this will actually contain our query
				// version, so add it back.
				i++;
			}
			// only include delta files after the snapshot file
			int j = i;
			while (j >= 0 && metadata->files.deltaFiles[j].version > snapshotVersion) {
				j--;
			}
			j++;
			Version latestDeltaVersion = invalidVersion;
			while (j <= i) {
				BlobFileIndex deltaF = metadata->files.deltaFiles[j];
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

ACTOR Future<GranuleChangeFeedInfo> persistAssignWorkerRange(BlobWorkerData* bwData, AssignBlobRangeRequest req) {
	ASSERT(!req.continueAssignment);
	state Transaction tr(bwData->db);
	state Key lockKey = granuleLockKey(req.keyRange);
	state GranuleChangeFeedInfo info;
	if (BW_DEBUG) {
		printf("%s persisting assignment [%s - %s)\n",
		       bwData->id.toString().c_str(),
		       req.keyRange.begin.printable().c_str(),
		       req.keyRange.end.printable().c_str());
	}

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			// FIXME: could add list of futures and do the different parts that are disjoint in parallel?
			info.changeFeedStartVersion = invalidVersion;
			Optional<Value> prevLockValue = wait(tr.get(lockKey));
			state bool hasPrevOwner = prevLockValue.present();
			if (hasPrevOwner) {
				std::tuple<int64_t, int64_t, UID> prevOwner = decodeBlobGranuleLockValue(prevLockValue.get());
				acquireGranuleLock(req.managerEpoch, req.managerSeqno, std::get<0>(prevOwner), std::get<1>(prevOwner));
				info.changeFeedId = std::get<2>(prevOwner);

				GranuleFiles granuleFiles = wait(loadPreviousFiles(&tr, req.keyRange));
				info.existingFiles = granuleFiles;
				info.doSnapshot = false;

				if (info.existingFiles.get().snapshotFiles.empty()) {
					ASSERT(info.existingFiles.get().deltaFiles.empty());
					info.previousDurableVersion = invalidVersion;
					info.doSnapshot = true;
				} else if (info.existingFiles.get().deltaFiles.empty()) {
					info.previousDurableVersion = info.existingFiles.get().snapshotFiles.back().version;
				} else {
					info.previousDurableVersion = info.existingFiles.get().deltaFiles.back().version;
				}

				// for the non-splitting cases, this doesn't need to be 100% accurate, it just needs to be smaller
				// than the next delta file write.
				info.changeFeedStartVersion = info.previousDurableVersion;
			} else {
				// else we are first, no need to check for owner conflict
				// FIXME: use actual 16 bytes of UID instead of converting it to 32 character string and then that
				// to bytes
				info.changeFeedId = deterministicRandom()->randomUniqueID();
				wait(tr.registerChangeFeed(StringRef(info.changeFeedId.toString()), req.keyRange));
				info.doSnapshot = true;
				info.previousDurableVersion = invalidVersion;
			}

			tr.set(lockKey, blobGranuleLockValueFor(req.managerEpoch, req.managerSeqno, info.changeFeedId));
			wait(krmSetRange(&tr, blobGranuleMappingKeys.begin, req.keyRange, blobGranuleMappingValueFor(bwData->id)));

			Tuple historyKey;
			historyKey.append(req.keyRange.begin).append(req.keyRange.end);
			Optional<Value> parentGranulesValue =
			    wait(tr.get(historyKey.getDataAsStandalone().withPrefix(blobGranuleHistoryKeys.begin)));

			// If anything in previousGranules, need to do the handoff logic and set ret.previousChangeFeedId, and
			// the previous durable version will come from the previous granules
			if (parentGranulesValue.present()) {
				state Standalone<VectorRef<KeyRangeRef>> parentGranules =
				    decodeBlobGranuleHistoryValue(parentGranulesValue.get());
				// TODO REMOVE
				if (BW_DEBUG) {
					printf("Decoded parent granules for [%s - %s)\n",
					       req.keyRange.begin.printable().c_str(),
					       req.keyRange.end.printable().c_str());
					for (auto& pg : parentGranules) {
						printf("  [%s - %s)\n", pg.begin.printable().c_str(), pg.end.printable().c_str());
					}
				}

				// TODO change this for merge
				ASSERT(parentGranules.size() == 1);

				state std::pair<BlobGranuleSplitState, Version> granuleSplitState;
				if (hasPrevOwner) {
					std::pair<BlobGranuleSplitState, Version> _st =
					    wait(getGranuleSplitState(&tr, parentGranules[0], req.keyRange));
					granuleSplitState = _st;
				} else {
					granuleSplitState = std::pair(BlobGranuleSplitState::Started, invalidVersion);
				}

				ASSERT(!hasPrevOwner || granuleSplitState.first > BlobGranuleSplitState::Started);

				// if granule wasn't done with old change feed, load it
				if (granuleSplitState.first < BlobGranuleSplitState::Done) {
					Optional<Value> prevGranuleLockValue = wait(tr.get(granuleLockKey(parentGranules[0])));
					ASSERT(prevGranuleLockValue.present());
					std::tuple<int64_t, int64_t, UID> prevGranuleLock =
					    decodeBlobGranuleLockValue(prevGranuleLockValue.get());
					info.prevChangeFeedId = std::get<2>(prevGranuleLock);
					info.granuleSplitFrom = parentGranules[0];

					if (granuleSplitState.first == BlobGranuleSplitState::Assigned) {
						// was already assigned, use change feed start version
						ASSERT(granuleSplitState.second != invalidVersion);
						info.changeFeedStartVersion = granuleSplitState.second;
					} else if (granuleSplitState.first == BlobGranuleSplitState::Started) {
						wait(updateGranuleSplitState(&tr,
						                             parentGranules[0],
						                             req.keyRange,
						                             info.prevChangeFeedId.get(),
						                             BlobGranuleSplitState::Assigned));
						// change feed was created as part of this transaction, changeFeedStartVersion will be set
						// later
					} else {
						ASSERT(false);
					}
				}

				if (info.doSnapshot) {
					// only need to do snapshot if no files exist yet for this granule.
					ASSERT(info.previousDurableVersion == invalidVersion);
					// FIXME: store this somewhere useful for time travel reads
					GranuleFiles prevFiles = wait(loadPreviousFiles(&tr, parentGranules[0]));
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
			} else {
				ASSERT(info.changeFeedStartVersion != invalidVersion);
			}

			TraceEvent("BlobWorkerPersistedAssignment", bwData->id).detail("Granule", req.keyRange);

			return info;
		} catch (Error& e) {
			if (e.code() == error_code_granule_assignment_conflict) {
				throw e;
			}
			wait(tr.onError(e));
		}
	}
}

static GranuleRangeMetadata constructActiveBlobRange(BlobWorkerData* bwData,
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
static std::pair<Future<Void>, Reference<GranuleMetadata>> changeBlobRange(BlobWorkerData* bwData,
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

	std::vector<Future<Void>> futures;

	std::vector<std::pair<KeyRange, GranuleRangeMetadata>> newerRanges;

	auto ranges = bwData->granuleMetadata.intersectingRanges(keyRange);
	for (auto& r : ranges) {
		bool thisAssignmentNewer = newerRangeAssignment(r.value(), epoch, seqno);
		if (r.value().lastEpoch == epoch && r.value().lastSeqno == seqno) {
			ASSERT(r.begin() == keyRange.begin);
			ASSERT(r.end() == keyRange.end);

			if (selfReassign) {
				thisAssignmentNewer = true;
			} else {
				// applied the same assignment twice, make idempotent
				if (r.value().activeMetadata.isValid()) {
					futures.push_back(success(r.value().activeMetadata->assignFuture));
				}
				return std::pair(waitForAll(futures),
				                 Reference<GranuleMetadata>()); // already applied, nothing to do
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
			futures.push_back(r.value().activeMetadata->cancel(disposeOnCleanup));
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

	return std::pair(waitForAll(futures), newMetadata.activeMetadata);
}

static bool resumeBlobRange(BlobWorkerData* bwData, KeyRange keyRange, int64_t epoch, int64_t seqno) {
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

ACTOR Future<Void> handleRangeAssign(BlobWorkerData* bwData, AssignBlobRangeRequest req, bool isSelfReassign) {
	try {
		if (req.continueAssignment) {
			resumeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno);
		} else {
			state std::pair<Future<Void>, Reference<GranuleMetadata>> futureAndNewGranule =
			    changeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno, true, false, isSelfReassign);

			wait(futureAndNewGranule.first);

			if (futureAndNewGranule.second.isValid()) {
				wait(futureAndNewGranule.second->start(bwData, req));
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

ACTOR Future<Void> handleRangeRevoke(BlobWorkerData* bwData, RevokeBlobRangeRequest req) {
	try {
		wait(
		    changeBlobRange(bwData, req.keyRange, req.managerEpoch, req.managerSeqno, false, req.dispose, false).first);
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

// FIXME: handle errors
// Because change feeds send uncommitted data and explicit rollback messages, we speculatively buffer/write
// uncommitted data. This means we must ensure the data is actually committed before "committing" those writes in
// the blob granule. The simplest way to do this is to have the blob worker do a periodic GRV, which is guaranteed
// to be an earlier committed version.
ACTOR Future<Void> runCommitVersionChecks(BlobWorkerData* bwData) {
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

		// register the blob worker to the system keyspace
		wait(registerBlobWorker(&self, bwInterf));
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

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());

	addActor.send(waitFailureServer(bwInterf.waitFailure.getFuture()));
	addActor.send(runCommitVersionChecks(&self));

	try {
		loop choose {
			when(BlobGranuleFileRequest req = waitNext(bwInterf.blobGranuleFileRequest.getFuture())) {
				/*printf("Got blob granule request [%s - %s)\n",
				       req.keyRange.begin.printable().c_str(),
				       req.keyRange.end.printable().c_str());*/
				++self.stats.readRequests;
				++self.stats.activeReadRequests;
				addActor.send(handleBlobGranuleFileRequest(&self, req));
			}
			when(GranuleStatusStreamRequest req = waitNext(bwInterf.granuleStatusStreamRequest.getFuture())) {
				if (self.managerEpochOk(req.managerEpoch)) {
					if (BW_DEBUG) {
						printf("Worker %s got new granule status endpoint\n", self.id.toString().c_str());
					}
					self.currentManagerStatusStream = req.reply;
				}
			}
			when(AssignBlobRangeRequest _req = waitNext(bwInterf.assignBlobRangeRequest.getFuture())) {
				++self.stats.rangeAssignmentRequests;
				--self.stats.numRangesAssigned;
				state AssignBlobRangeRequest assignReq = _req;
				if (BW_DEBUG) {
					printf("Worker %s assigned range [%s - %s) @ (%lld, %lld):\n  continue=%s\n",
					       self.id.toString().c_str(),
					       assignReq.keyRange.begin.printable().c_str(),
					       assignReq.keyRange.end.printable().c_str(),
					       assignReq.managerEpoch,
					       assignReq.managerSeqno,
					       assignReq.continueAssignment ? "T" : "F");
				}

				if (self.managerEpochOk(assignReq.managerEpoch)) {
					addActor.send(handleRangeAssign(&self, assignReq, false));
				} else {
					assignReq.reply.send(AssignBlobRangeReply(false));
				}
			}
			when(RevokeBlobRangeRequest _req = waitNext(bwInterf.revokeBlobRangeRequest.getFuture())) {
				state RevokeBlobRangeRequest revokeReq = _req;
				--self.stats.numRangesAssigned;
				if (BW_DEBUG) {
					printf("Worker %s revoked range [%s - %s) @ (%lld, %lld):\n  dispose=%s\n",
					       self.id.toString().c_str(),
					       revokeReq.keyRange.begin.printable().c_str(),
					       revokeReq.keyRange.end.printable().c_str(),
					       revokeReq.managerEpoch,
					       revokeReq.managerSeqno,
					       revokeReq.dispose ? "T" : "F");
				}

				if (self.managerEpochOk(revokeReq.managerEpoch)) {
					addActor.send(handleRangeRevoke(&self, revokeReq));
				} else {
					revokeReq.reply.send(AssignBlobRangeReply(false));
				}
			}
			when(AssignBlobRangeRequest granuleToReassign = waitNext(self.granuleUpdateErrors.getFuture())) {
				addActor.send(handleRangeAssign(&self, granuleToReassign, true));
			}
			when(wait(collection)) {
				if (BW_DEBUG) {
					printf("BW actor collection returned, exiting\n");
				}
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		if (BW_DEBUG) {
			printf("Blob worker got error %s, exiting\n", e.name());
		}
		TraceEvent("BlobWorkerDied", self.id).error(e, true);
		throw e;
	}
}

// TODO add unit tests for assign/revoke range, especially version ordering
