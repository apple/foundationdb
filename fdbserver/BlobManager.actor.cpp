/*
 * BlobManager.actor.cpp
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

#include <vector>
#include <unordered_map>

#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BlobManagerInterface.h"
#include "fdbserver/BlobWorker.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

#define BM_DEBUG 1

// TODO add comments + documentation
void handleClientBlobRange(KeyRangeMap<bool>* knownBlobRanges,
                           Arena ar,
                           VectorRef<KeyRangeRef>* rangesToAdd,
                           VectorRef<KeyRangeRef>* rangesToRemove,
                           KeyRef rangeStart,
                           KeyRef rangeEnd,
                           bool rangeActive) {
	if (BM_DEBUG) {
		printf("db range [%s - %s): %s\n",
		       rangeStart.printable().c_str(),
		       rangeEnd.printable().c_str(),
		       rangeActive ? "T" : "F");
	}
	KeyRange keyRange(KeyRangeRef(rangeStart, rangeEnd));
	auto allRanges = knownBlobRanges->intersectingRanges(keyRange);
	for (auto& r : allRanges) {
		if (r.value() != rangeActive) {
			KeyRef overlapStart = (r.begin() > keyRange.begin) ? r.begin() : keyRange.begin;
			KeyRef overlapEnd = (keyRange.end < r.end()) ? keyRange.end : r.end();
			KeyRangeRef overlap(overlapStart, overlapEnd);
			if (rangeActive) {
				if (BM_DEBUG) {
					printf("BM Adding client range [%s - %s)\n",
					       overlapStart.printable().c_str(),
					       overlapEnd.printable().c_str());
				}
				rangesToAdd->push_back_deep(ar, overlap);
			} else {
				if (BM_DEBUG) {
					printf("BM Removing client range [%s - %s)\n",
					       overlapStart.printable().c_str(),
					       overlapEnd.printable().c_str());
				}
				rangesToRemove->push_back_deep(ar, overlap);
			}
		}
	}
	knownBlobRanges->insert(keyRange, rangeActive);
}

void updateClientBlobRanges(KeyRangeMap<bool>* knownBlobRanges,
                            RangeResult dbBlobRanges,
                            Arena ar,
                            VectorRef<KeyRangeRef>* rangesToAdd,
                            VectorRef<KeyRangeRef>* rangesToRemove) {
	if (BM_DEBUG) {
		printf("Updating %d client blob ranges", dbBlobRanges.size() / 2);
		for (int i = 0; i < dbBlobRanges.size() - 1; i += 2) {
			printf(" [%s - %s)", dbBlobRanges[i].key.printable().c_str(), dbBlobRanges[i + 1].key.printable().c_str());
		}
		printf("\n");
	}
	// essentially do merge diff of current known blob ranges and new ranges, to assign new ranges to
	// workers and revoke old ranges from workers

	// basically, for any range that is set in results that isn't set in ranges, assign the range to the
	// worker. for any range that isn't set in results that is set in ranges, revoke the range from the
	// worker. and, update ranges to match results as you go

	// FIXME: could change this to O(N) instead of O(NLogN) by doing a sorted merge instead of requesting the
	// intersection for each insert, but this operation is pretty infrequent so it's probably not necessary
	if (dbBlobRanges.size() == 0) {
		// special case. Nothing in the DB, reset knownBlobRanges and revoke all existing ranges from workers
		handleClientBlobRange(
		    knownBlobRanges, ar, rangesToAdd, rangesToRemove, normalKeys.begin, normalKeys.end, false);
	} else {
		if (dbBlobRanges[0].key > normalKeys.begin) {
			handleClientBlobRange(
			    knownBlobRanges, ar, rangesToAdd, rangesToRemove, normalKeys.begin, dbBlobRanges[0].key, false);
		}
		for (int i = 0; i < dbBlobRanges.size() - 1; i++) {
			if (dbBlobRanges[i].key >= normalKeys.end) {
				if (BM_DEBUG) {
					printf("Found invalid blob range start %s\n", dbBlobRanges[i].key.printable().c_str());
				}
				break;
			}
			bool active = dbBlobRanges[i].value == LiteralStringRef("1");
			if (active) {
				ASSERT(dbBlobRanges[i + 1].value == StringRef());
				if (BM_DEBUG) {
					printf("BM sees client range [%s - %s)\n",
					       dbBlobRanges[i].key.printable().c_str(),
					       dbBlobRanges[i + 1].key.printable().c_str());
				}
			}
			KeyRef endKey = dbBlobRanges[i + 1].key;
			if (endKey > normalKeys.end) {
				if (BM_DEBUG) {
					printf("Removing system keyspace from blob range [%s - %s)\n",
					       dbBlobRanges[i].key.printable().c_str(),
					       endKey.printable().c_str());
				}
				endKey = normalKeys.end;
			}
			handleClientBlobRange(
			    knownBlobRanges, ar, rangesToAdd, rangesToRemove, dbBlobRanges[i].key, endKey, active);
		}
		if (dbBlobRanges[dbBlobRanges.size() - 1].key < normalKeys.end) {
			handleClientBlobRange(knownBlobRanges,
			                      ar,
			                      rangesToAdd,
			                      rangesToRemove,
			                      dbBlobRanges[dbBlobRanges.size() - 1].key,
			                      normalKeys.end,
			                      false);
		}
	}
	knownBlobRanges->coalesce(normalKeys);
}

void getRanges(std::vector<std::pair<KeyRangeRef, bool>>& results, KeyRangeMap<bool>& knownBlobRanges) {
	if (BM_DEBUG) {
		printf("Getting ranges:\n");
	}
	auto allRanges = knownBlobRanges.ranges();
	for (auto& r : allRanges) {
		results.emplace_back(r.range(), r.value());
		if (BM_DEBUG) {
			printf(
			    "  [%s - %s): %s\n", r.begin().printable().c_str(), r.end().printable().c_str(), r.value() ? "T" : "F");
		}
	}
}

// Assigns and revokes have slightly different semantics. Revokes are idempotent revoking from a particular worker, and
// will be retried to the same worker. Assigns are not to a specific worker. An initial worker will be chosen, and if it
// fails, the range will be revoked from that worker and put back in the queue, where it will then eventually be
// assigned to a different worker.
struct RangeAssignment {
	KeyRange keyRange;
	bool isAssign;

	RangeAssignment() {}
	explicit RangeAssignment(KeyRange keyRange, bool isAssign) : keyRange(keyRange), isAssign(isAssign) {}
};

// TODO: track worker's reads/writes eventually
struct BlobWorkerStats {
	int numGranulesAssigned;

	BlobWorkerStats(int numGranulesAssigned=0): numGranulesAssigned(numGranulesAssigned) {}
};

struct BlobManagerData {
	UID id;
	Database db;

	std::unordered_map<UID, BlobWorkerInterface> workersById;
	std::unordered_map<UID, BlobWorkerStats> workerStats; // mapping between workerID -> workerStats
	KeyRangeMap<UID> workerAssignments;
	KeyRangeMap<bool> knownBlobRanges;

	int64_t epoch = -1;
	int64_t seqNo = 1;

	Promise<Void> iAmReplaced;

	// The order maintained here is important. The order ranges are put into the promise stream is the order they get
	// assigned sequence numbers
	PromiseStream<RangeAssignment> rangesToAssign;

	BlobManagerData(UID id, Database db) : id(id), db(db), knownBlobRanges(false, normalKeys.end) {}
	~BlobManagerData() { printf("Destroying blob manager data for %s\n", id.toString().c_str()); }
};

// TODO REMOVE eventually
ACTOR Future<Void> nukeBlobWorkerData(BlobManagerData* bmData) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {
			tr->clear(blobWorkerListKeys);
			tr->clear(blobGranuleMappingKeys);
			tr->clear(rangeFeedKeys);

			return Void();
		} catch (Error& e) {
			if (BM_DEBUG) {
				printf("Nuking blob worker data got error %s\n", e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Standalone<VectorRef<KeyRef>>> splitNewRange(Reference<ReadYourWritesTransaction> tr, KeyRange range) {
	// TODO is it better to just pass empty metrics to estimated?
	// TODO handle errors here by pulling out into its own transaction instead of the main loop's transaction, and
	// retrying
	if (BM_DEBUG) {
		printf("Splitting new range [%s - %s)\n", range.begin.printable().c_str(), range.end.printable().c_str());
	}
	StorageMetrics estimated = wait(tr->getTransaction().getStorageMetrics(range, CLIENT_KNOBS->TOO_MANY));

	if (BM_DEBUG) {
		printf("Estimated bytes for [%s - %s): %lld\n",
		       range.begin.printable().c_str(),
		       range.end.printable().c_str(),
		       estimated.bytes);
	}

	if (estimated.bytes > SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES) {
		// printf("  Splitting range\n");
		// only split on bytes
		StorageMetrics splitMetrics;
		splitMetrics.bytes = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
		splitMetrics.bytesPerKSecond = splitMetrics.infinity;
		splitMetrics.iosPerKSecond = splitMetrics.infinity;
		splitMetrics.bytesReadPerKSecond = splitMetrics.infinity; // Don't split by readBandwidth

		Standalone<VectorRef<KeyRef>> keys =
		    wait(tr->getTransaction().splitStorageMetrics(range, splitMetrics, estimated));
		return keys;
	} else {
		// printf("  Not splitting range\n");
		Standalone<VectorRef<KeyRef>> keys;
		keys.push_back_deep(keys.arena(), range.begin);
		keys.push_back_deep(keys.arena(), range.end);
		return keys;
	}
}

// Picks a worker with the fewest number of already assigned ranges.
// If there is a tie, picks one such worker at random.
static UID pickWorkerForAssign(BlobManagerData* bmData) {
	int minGranulesAssigned = INT_MAX;
	std::vector<UID> eligibleWorkers;
	
	for (auto const &worker : bmData->workerStats) {
		UID currId = worker.first;
		int granulesAssigned = worker.second.numGranulesAssigned;

		if (granulesAssigned < minGranulesAssigned) {
			eligibleWorkers.resize(0);
			minGranulesAssigned = granulesAssigned;
			eligibleWorkers.emplace_back(currId);
		} else if (granulesAssigned == minGranulesAssigned) {
			eligibleWorkers.emplace_back(currId);
		}
	}

	// pick a random worker out of the eligible workers
	ASSERT(eligibleWorkers.size() > 0);
	int idx = deterministicRandom()->randomInt(0, eligibleWorkers.size());
	if (BM_DEBUG) {
		printf("picked worker %s, which has a minimal number (%d) of granules assigned\n", 
			   eligibleWorkers[idx].toString().c_str(), minGranulesAssigned);
	}

	return eligibleWorkers[idx];
}

ACTOR Future<Void> doRangeAssignment(BlobManagerData* bmData, RangeAssignment assignment, UID workerID, int64_t seqNo) {
	AssignBlobRangeRequest req;
	req.keyRange =
	    KeyRangeRef(StringRef(req.arena, assignment.keyRange.begin), StringRef(req.arena, assignment.keyRange.end));
	req.managerEpoch = bmData->epoch;
	req.managerSeqno = seqNo;
	req.isAssign = assignment.isAssign;

	if (BM_DEBUG) {
		printf("BM %s %s range [%s - %s) @ (%lld, %lld)\n",
		       workerID.toString().c_str(),
		       req.isAssign ? "assigning" : "revoking",
		       req.keyRange.begin.printable().c_str(),
		       req.keyRange.end.printable().c_str(),
		       req.managerEpoch,
		       req.managerSeqno);
	}

	try {
		AssignBlobRangeReply rep = wait(bmData->workersById[workerID].assignBlobRangeRequest.getReply(req));
		if (!rep.epochOk) {
			if (BM_DEBUG) {
				printf("BM heard from BW that there is a new manager with higher epoch\n");
			}
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.send(Void());
			}
		}
	} catch (Error& e) {
		// TODO confirm: using reliable delivery this should only trigger if the worker is marked as failed, right?
		// So assignment needs to be retried elsewhere, and a revoke is trivially complete
		if (assignment.isAssign) {
			if (BM_DEBUG) {
				printf("BM got error assigning range [%s - %s) to worker %s, requeueing\n",
				       assignment.keyRange.begin.printable().c_str(),
				       assignment.keyRange.end.printable().c_str());
			}
			// re-send revoke to queue to handle range being un-assigned from that worker before the new one
			bmData->rangesToAssign.send(RangeAssignment(assignment.keyRange, false));
			bmData->rangesToAssign.send(assignment);
			// FIXME: improvement would be to add history of failed workers to assignment so it can try other ones first
		} else if (BM_DEBUG) {
			printf("BM got error revoking range [%s - %s) from worker %s, ignoring\n",
			       assignment.keyRange.begin.printable().c_str(),
			       assignment.keyRange.end.printable().c_str());
		}
	}
	return Void();
}

ACTOR Future<Void> rangeAssigner(BlobManagerData* bmData) {
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());
	loop {
		RangeAssignment assignment = waitNext(bmData->rangesToAssign.getFuture());
		int64_t seqNo = bmData->seqNo;
		bmData->seqNo++;

		// modify the in-memory assignment data structures, and send request off to worker
		UID workerId;
		if (assignment.isAssign) {
			// Ensure range isn't currently assigned anywhere, and there is only 1 intersecting range
			auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
			int count = 0;
			for (auto& it : currentAssignments) {
				ASSERT(it.value() == UID());
				count++;
			}
			ASSERT(count == 1);

			workerId = pickWorkerForAssign(bmData);
			bmData->workerAssignments.insert(assignment.keyRange, workerId);
			bmData->workerStats[workerId].numGranulesAssigned += 1;

			// FIXME: if range is assign, have some sort of semaphore for outstanding assignments so we don't assign
			// a ton ranges at once and blow up FDB with reading initial snapshots.
			addActor.send(doRangeAssignment(bmData, assignment, workerId, seqNo));
		} else {
			// Revoking a range could be a large range that contains multiple ranges.
			auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
			for (auto& it : currentAssignments) {
				// ensure range doesn't truncate existing ranges
				ASSERT(it.begin() >= assignment.keyRange.begin);
				ASSERT(it.end() <= assignment.keyRange.end);

				// It is fine for multiple disjoint sub-ranges to have the same sequence number since they were part of
				// the same logical change
				bmData->workerStats[it.value()].numGranulesAssigned -= 1;
				addActor.send(doRangeAssignment(bmData, assignment, it.value(), seqNo));
			}

			bmData->workerAssignments.insert(assignment.keyRange, UID());
		}
	}
}

// TODO eventually CC should probably do this and pass it as part of recruitment?
ACTOR Future<int64_t> acquireManagerLock(BlobManagerData* bmData) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {
			// TODO verify: this should automatically have a read conflict range for blobManagerEpochKey, right?
			Optional<Value> oldEpoch = wait(tr->get(blobManagerEpochKey));
			state int64_t newEpoch;
			if (oldEpoch.present()) {
				newEpoch = decodeBlobManagerEpochValue(oldEpoch.get()) + 1;
			} else {
				newEpoch = 1; // start at 1
			}

			tr->set(blobManagerEpochKey, blobManagerEpochValueFor(newEpoch));

			wait(tr->commit());
			return newEpoch;
		} catch (Error& e) {
			if (BM_DEBUG) {
				printf("Acquiring blob manager lock got error %s\n", e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> monitorClientRanges(BlobManagerData* bmData) {
	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

		if (BM_DEBUG) {
			printf("Blob manager checking for range updates\n");
		}
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				// TODO probably knobs here? This should always be pretty small though
				RangeResult results = wait(krmGetRanges(
				    tr, blobRangeKeys.begin, KeyRange(normalKeys), 10000, GetRangeLimits::BYTE_LIMIT_UNLIMITED));
				ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

				state Arena ar;
				ar.dependsOn(results.arena());
				VectorRef<KeyRangeRef> rangesToAdd;
				VectorRef<KeyRangeRef> rangesToRemove;
				// TODO hack for simulation
				updateClientBlobRanges(&bmData->knownBlobRanges, results, ar, &rangesToAdd, &rangesToRemove);

				for (KeyRangeRef range : rangesToRemove) {
					if (BM_DEBUG) {
						printf("BM Got range to revoke [%s - %s)\n",
						       range.begin.printable().c_str(),
						       range.end.printable().c_str());
					}

					bmData->rangesToAssign.send(RangeAssignment(range, false));
				}

				state std::vector<Future<Standalone<VectorRef<KeyRef>>>> splitFutures;
				// Divide new ranges up into equal chunks by using SS byte sample
				for (KeyRangeRef range : rangesToAdd) {
					// assert that this range contains no currently assigned ranges in this
					splitFutures.push_back(splitNewRange(tr, range));
				}

				for (auto f : splitFutures) {
					Standalone<VectorRef<KeyRef>> splits = wait(f);
					if (BM_DEBUG) {
						printf("Split client range [%s - %s) into %d ranges:\n",
						       splits[0].printable().c_str(),
						       splits[splits.size() - 1].printable().c_str(),
						       splits.size() - 1);
					}

					for (int i = 0; i < splits.size() - 1; i++) {
						KeyRange range = KeyRange(KeyRangeRef(splits[i], splits[i + 1]));
						if (BM_DEBUG) {
							printf("    [%s - %s)\n", range.begin.printable().c_str(), range.end.printable().c_str());
						}

						bmData->rangesToAssign.send(RangeAssignment(range, true));
					}
				}

				state Future<Void> watchFuture = tr->watch(blobRangeChangeKey);
				wait(tr->commit());
				if (BM_DEBUG) {
					printf("Blob manager done processing client ranges, awaiting update\n");
				}
				wait(watchFuture);
				break;
			} catch (Error& e) {
				if (BM_DEBUG) {
					printf("Blob manager got error looking for range updates %s\n", e.name());
				}
				wait(tr->onError(e));
			}
		}
	}
}

// TODO this is only for chaos testing right now!! REMOVE LATER
ACTOR Future<Void> rangeMover(BlobManagerData* bmData) {
	loop {
		wait(delay(30.0));

		if (bmData->workersById.size() > 1) {
			int tries = 10;
			while (tries > 0) {
				tries--;
				auto randomRange = bmData->workerAssignments.randomRange();
				if (randomRange.value() != UID()) {
					if (BM_DEBUG) {
						printf("Range mover moving range [%s - %s): %s\n",
						       randomRange.begin().printable().c_str(),
						       randomRange.end().printable().c_str(),
						       randomRange.value().toString().c_str());
					}

					bmData->rangesToAssign.send(RangeAssignment(randomRange.range(), false));
					bmData->rangesToAssign.send(RangeAssignment(randomRange.range(), true));
					break;
				}
			}
			if (tries == 0 && BM_DEBUG) {
				printf("Range mover couldn't find range to move, skipping\n");
			}
		} else if (BM_DEBUG) {
			printf("Range mover found %d workers, skipping\n", bmData->workerAssignments.size());
		}
	}
}

// TODO MOVE ELSEWHERE
// TODO replace locality with full BlobManagerInterface eventually
ACTOR Future<Void> blobManager(LocalityData locality, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state BlobManagerData self(deterministicRandom()->randomUniqueID(),
	                           openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True));

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());

	// TODO remove once we have persistence + failure detection
	if (BM_DEBUG) {
		printf("Blob manager nuking previous workers and range assignments on startup\n");
	}
	wait(nukeBlobWorkerData(&self));

	if (BM_DEBUG) {
		printf("Blob manager nuked previous workers and range assignments\n");
		printf("Blob manager taking lock\n");
	}

	int64_t _epoch = wait(acquireManagerLock(&self));
	self.epoch = _epoch;
	if (BM_DEBUG) {
		printf("Blob manager acquired lock at epoch %lld\n", _epoch);
	}

	int numWorkers = 2;
	for (int i = 0; i < numWorkers; i++) {
		state BlobWorkerInterface bwInterf(locality, deterministicRandom()->randomUniqueID());
		bwInterf.initEndpoints();
		self.workersById.insert({ bwInterf.id(), bwInterf });
		self.workerStats.insert({ bwInterf.id(), BlobWorkerStats() });
		addActor.send(blobWorker(bwInterf, dbInfo));
	}

	addActor.send(monitorClientRanges(&self));
	addActor.send(rangeAssigner(&self));

	// TODO add back once everything is properly implemented!
	// addActor.send(rangeMover(&self));

	// TODO probably other things here eventually
	loop choose {
		when(wait(self.iAmReplaced.getFuture())) {
			if (BM_DEBUG) {
				printf("Blob Manager exiting because it is replaced\n");
			}
			return Void();
		}
	}
}

// Test:
// start empty
// DB has [A - B). That should show up in knownBlobRanges and should be in added
// DB has nothing. knownBlobRanges should be empty and [A - B) should be in removed
// DB has [A - B) and [C - D). They should both show up in knownBlobRanges and added.
// DB has [A - D). It should show up coalesced in knownBlobRanges, and [B - C) should be in added.
// DB has [A - C). It should show up coalesced in knownBlobRanges, and [C - D) should be in removed.
// DB has [B - C). It should show up coalesced in knownBlobRanges, and [A - B) should be removed.
// DB has [B - D). It should show up coalesced in knownBlobRanges, and [C - D) should be removed.
// DB has [A - D). It should show up coalesced in knownBlobRanges, and [A - B) should be removed.
// DB has [A - B) and [C - D). They should show up in knownBlobRanges, and [B - C) should be in removed.
// DB has [B - C). It should show up in knownBlobRanges, [B - C) should be in added, and [A - B) and [C - D) should be
// in removed.
TEST_CASE("/blobmanager/updateranges") {
	KeyRangeMap<bool> knownBlobRanges(false, normalKeys.end);
	Arena ar;

	VectorRef<KeyRangeRef> added;
	VectorRef<KeyRangeRef> removed;

	StringRef active = LiteralStringRef("1");
	StringRef inactive = StringRef();

	RangeResult dbDataEmpty;
	vector<std::pair<KeyRangeRef, bool>> kbrRanges;

	StringRef keyA = StringRef(ar, LiteralStringRef("A"));
	StringRef keyB = StringRef(ar, LiteralStringRef("B"));
	StringRef keyC = StringRef(ar, LiteralStringRef("C"));
	StringRef keyD = StringRef(ar, LiteralStringRef("D"));

	// db data setup
	RangeResult dbDataAB;
	dbDataAB.emplace_back(ar, keyA, active);
	dbDataAB.emplace_back(ar, keyB, inactive);

	RangeResult dbDataAC;
	dbDataAC.emplace_back(ar, keyA, active);
	dbDataAC.emplace_back(ar, keyC, inactive);

	RangeResult dbDataAD;
	dbDataAD.emplace_back(ar, keyA, active);
	dbDataAD.emplace_back(ar, keyD, inactive);

	RangeResult dbDataBC;
	dbDataBC.emplace_back(ar, keyB, active);
	dbDataBC.emplace_back(ar, keyC, inactive);

	RangeResult dbDataBD;
	dbDataBD.emplace_back(ar, keyB, active);
	dbDataBD.emplace_back(ar, keyD, inactive);

	RangeResult dbDataCD;
	dbDataCD.emplace_back(ar, keyC, active);
	dbDataCD.emplace_back(ar, keyD, inactive);

	RangeResult dbDataAB_CD;
	dbDataAB_CD.emplace_back(ar, keyA, active);
	dbDataAB_CD.emplace_back(ar, keyB, inactive);
	dbDataAB_CD.emplace_back(ar, keyC, active);
	dbDataAB_CD.emplace_back(ar, keyD, inactive);

	// key ranges setup
	KeyRangeRef rangeAB = KeyRangeRef(keyA, keyB);
	KeyRangeRef rangeAC = KeyRangeRef(keyA, keyC);
	KeyRangeRef rangeAD = KeyRangeRef(keyA, keyD);

	KeyRangeRef rangeBC = KeyRangeRef(keyB, keyC);
	KeyRangeRef rangeBD = KeyRangeRef(keyB, keyD);

	KeyRangeRef rangeCD = KeyRangeRef(keyC, keyD);

	KeyRangeRef rangeStartToA = KeyRangeRef(normalKeys.begin, keyA);
	KeyRangeRef rangeStartToB = KeyRangeRef(normalKeys.begin, keyB);
	KeyRangeRef rangeStartToC = KeyRangeRef(normalKeys.begin, keyC);
	KeyRangeRef rangeBToEnd = KeyRangeRef(keyB, normalKeys.end);
	KeyRangeRef rangeCToEnd = KeyRangeRef(keyC, normalKeys.end);
	KeyRangeRef rangeDToEnd = KeyRangeRef(keyD, normalKeys.end);

	// actual test

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 1);
	ASSERT(kbrRanges[0].first == normalKeys);
	ASSERT(!kbrRanges[0].second);

	// DB has [A - B)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataAB, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeAB);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAB);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeBToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has nothing
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataEmpty, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeAB);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges[0].first == normalKeys);
	ASSERT(!kbrRanges[0].second);

	// DB has [A - B) and [C - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataAB_CD, ar, &added, &removed);

	ASSERT(added.size() == 2);
	ASSERT(added[0] == rangeAB);
	ASSERT(added[1] == rangeCD);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 5);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAB);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeBC);
	ASSERT(!kbrRanges[2].second);
	ASSERT(kbrRanges[3].first == rangeCD);
	ASSERT(kbrRanges[3].second);
	ASSERT(kbrRanges[4].first == rangeDToEnd);
	ASSERT(!kbrRanges[4].second);

	// DB has [A - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataAD, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeBC);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAD);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeDToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [A - C)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataAC, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeCD);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAC);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeCToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [B - C)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataBC, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeAB);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToB);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeBC);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeCToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [B - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataBD, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeCD);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToB);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeBD);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeDToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [A - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataAD, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeAB);

	ASSERT(removed.size() == 0);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAD);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeDToEnd);
	ASSERT(!kbrRanges[2].second);

	// DB has [A - B) and [C - D)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataAB_CD, ar, &added, &removed);

	ASSERT(added.size() == 0);

	ASSERT(removed.size() == 1);
	ASSERT(removed[0] == rangeBC);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 5);
	ASSERT(kbrRanges[0].first == rangeStartToA);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeAB);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeBC);
	ASSERT(!kbrRanges[2].second);
	ASSERT(kbrRanges[3].first == rangeCD);
	ASSERT(kbrRanges[3].second);
	ASSERT(kbrRanges[4].first == rangeDToEnd);
	ASSERT(!kbrRanges[4].second);

	// DB has [B - C)
	kbrRanges.clear();
	added.clear();
	removed.clear();
	updateClientBlobRanges(&knownBlobRanges, dbDataBC, ar, &added, &removed);

	ASSERT(added.size() == 1);
	ASSERT(added[0] == rangeBC);

	ASSERT(removed.size() == 2);
	ASSERT(removed[0] == rangeAB);
	ASSERT(removed[1] == rangeCD);

	getRanges(kbrRanges, knownBlobRanges);
	ASSERT(kbrRanges.size() == 3);
	ASSERT(kbrRanges[0].first == rangeStartToB);
	ASSERT(!kbrRanges[0].second);
	ASSERT(kbrRanges[1].first == rangeBC);
	ASSERT(kbrRanges[1].second);
	ASSERT(kbrRanges[2].first == rangeCToEnd);
	ASSERT(!kbrRanges[2].second);

	return Void();
}
