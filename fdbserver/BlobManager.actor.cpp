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
#include "fdbclient/Tuple.h"
#include "fdbserver/BlobManagerInterface.h"
#include "fdbserver/BlobWorker.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#define BM_DEBUG true

// FIXME: change all BlobManagerData* to Reference<BlobManagerData> to avoid segfaults if core loop gets error

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

struct RangeAssignmentData {
	bool continueAssignment;

	RangeAssignmentData() : continueAssignment(false) {}
	RangeAssignmentData(bool continueAssignment) : continueAssignment(continueAssignment) {}
};

struct RangeRevokeData {
	bool dispose;

	RangeRevokeData() {}
	RangeRevokeData(bool dispose) : dispose(dispose) {}
};

struct RangeAssignment {
	bool isAssign;
	KeyRange keyRange;
	Optional<UID> worker;

	// I tried doing this with a union and it was just kind of messy
	Optional<RangeAssignmentData> assign;
	Optional<RangeRevokeData> revoke;
};

// TODO: track worker's reads/writes eventually
struct BlobWorkerStats {
	int numGranulesAssigned;

	BlobWorkerStats(int numGranulesAssigned = 0) : numGranulesAssigned(numGranulesAssigned) {}
};

struct BlobManagerData {
	UID id;
	Database db;
	PromiseStream<Future<Void>> addActor;

	std::unordered_map<UID, BlobWorkerInterface> workersById;
	std::unordered_map<UID, BlobWorkerStats> workerStats; // mapping between workerID -> workerStats
	KeyRangeMap<UID> workerAssignments;
	KeyRangeMap<bool> knownBlobRanges;

	Debouncer restartRecruiting;
	std::set<NetworkAddress> recruitingLocalities; // the addrs of the workers being recruited on

	int64_t epoch = -1;
	int64_t seqNo = 1;

	Promise<Void> iAmReplaced;

	// The order maintained here is important. The order ranges are put into the promise stream is the order they get
	// assigned sequence numbers
	PromiseStream<RangeAssignment> rangesToAssign;

	BlobManagerData(UID id, Database db)
	  : id(id), db(db), knownBlobRanges(false, normalKeys.end),
	    restartRecruiting(SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY) {}
	~BlobManagerData() { printf("Destroying blob manager data for %s\n", id.toString().c_str()); }
};

ACTOR Future<Standalone<VectorRef<KeyRef>>> splitRange(Reference<ReadYourWritesTransaction> tr, KeyRange range) {
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

	for (auto const& worker : bmData->workerStats) {
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
	if (eligibleWorkers.size() == 0) {
		printf("%d eligible workers\n", bmData->workerStats.size());
	}

	ASSERT(eligibleWorkers.size() > 0);
	int idx = deterministicRandom()->randomInt(0, eligibleWorkers.size());
	if (BM_DEBUG) {
		printf("picked worker %s, which has a minimal number (%d) of granules assigned\n",
		       eligibleWorkers[idx].toString().c_str(),
		       minGranulesAssigned);
	}

	return eligibleWorkers[idx];
}

ACTOR Future<Void> doRangeAssignment(BlobManagerData* bmData, RangeAssignment assignment, UID workerID, int64_t seqNo) {

	if (BM_DEBUG) {
		printf("BM %s %s range [%s - %s) @ (%lld, %lld)\n",
		       bmData->id.toString().c_str(),
		       assignment.isAssign ? "assigning" : "revoking",
		       assignment.keyRange.begin.printable().c_str(),
		       assignment.keyRange.end.printable().c_str(),
		       bmData->epoch,
		       seqNo);
	}

	try {
		state AssignBlobRangeReply rep;
		if (assignment.isAssign) {
			ASSERT(assignment.assign.present());
			ASSERT(!assignment.revoke.present());

			AssignBlobRangeRequest req;
			req.keyRange = KeyRangeRef(StringRef(req.arena, assignment.keyRange.begin),
			                           StringRef(req.arena, assignment.keyRange.end));
			req.managerEpoch = bmData->epoch;
			req.managerSeqno = seqNo;
			req.continueAssignment = assignment.assign.get().continueAssignment;

			// if that worker isn't alive anymore, add the range back into the stream
			if (bmData->workersById.count(workerID) == 0) {
				throw granule_assignment_conflict(); // TODO: find a better error to throw
			}
			AssignBlobRangeReply _rep = wait(bmData->workersById[workerID].assignBlobRangeRequest.getReply(req));
			rep = _rep;
		} else {
			ASSERT(!assignment.assign.present());
			ASSERT(assignment.revoke.present());

			RevokeBlobRangeRequest req;
			req.keyRange = KeyRangeRef(StringRef(req.arena, assignment.keyRange.begin),
			                           StringRef(req.arena, assignment.keyRange.end));
			req.managerEpoch = bmData->epoch;
			req.managerSeqno = seqNo;
			req.dispose = assignment.revoke.get().dispose;

			// if that worker isn't alive anymore, this is a noop
			if (bmData->workersById.count(workerID)) {
				AssignBlobRangeReply _rep = wait(bmData->workersById[workerID].revokeBlobRangeRequest.getReply(req));
				rep = _rep;
			} else {
				return Void();
			}
		}
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
				       assignment.keyRange.end.printable().c_str(),
				       workerID.toString().c_str());
			}
			// re-send revoke to queue to handle range being un-assigned from that worker before the new one
			RangeAssignment revokeOld;
			revokeOld.isAssign = false;
			revokeOld.worker = workerID;
			revokeOld.keyRange = assignment.keyRange;
			revokeOld.revoke = RangeRevokeData(false);
			bmData->rangesToAssign.send(revokeOld);

			// send assignment back to queue as is, clearing designated worker if present
			assignment.worker.reset();
			bmData->rangesToAssign.send(assignment);
			// FIXME: improvement would be to add history of failed workers to assignment so it can try other ones first
		} else {
			if (BM_DEBUG) {
				printf("BM got error revoking range [%s - %s) from worker %s",
				       assignment.keyRange.begin.printable().c_str(),
				       assignment.keyRange.end.printable().c_str());
			}

			if (assignment.revoke.get().dispose) {
				if (BM_DEBUG) {
					printf(", retrying for dispose\n");
				}
				// send assignment back to queue as is, clearing designated worker if present
				assignment.worker.reset();
				bmData->rangesToAssign.send(assignment);
				//
			} else {
				if (BM_DEBUG) {
					printf(", ignoring\n");
				}
			}
		}
	}
	return Void();
}

ACTOR Future<Void> rangeAssigner(BlobManagerData* bmData) {
	loop {
		// inject delay into range assignments
		if (BUGGIFY_WITH_PROB(0.05)) {
			wait(delay(deterministicRandom()->random01()));
		}
		RangeAssignment assignment = waitNext(bmData->rangesToAssign.getFuture());
		int64_t seqNo = bmData->seqNo;
		bmData->seqNo++;

		// modify the in-memory assignment data structures, and send request off to worker
		UID workerId;
		if (assignment.isAssign) {
			// Ensure range isn't currently assigned anywhere, and there is only 1 intersecting range
			auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
			int count = 0;
			printf("intersecting ranges in currentAssignments:\n");
			for (auto& it : currentAssignments) {
				printf("[%s - %s]\n", it.begin().printable().c_str(), it.end().printable().c_str());
				if (assignment.assign.get().continueAssignment) {
					ASSERT(assignment.worker.present());
					ASSERT(it.value() == assignment.worker.get());
				} else {
					ASSERT(it.value() == UID());
				}
				count++;
			}
			ASSERT(count == 1);

			workerId = assignment.worker.present() ? assignment.worker.get() : pickWorkerForAssign(bmData);
			bmData->workerAssignments.insert(assignment.keyRange, workerId);
			if (bmData->workerStats.count(workerId)) {
				bmData->workerStats[workerId].numGranulesAssigned += 1;
			}

			printf("current ranges after inserting assign: \n");
			for (auto it : bmData->workerAssignments.ranges()) {
				printf("[%s - %s]\n", it.begin().printable().c_str(), it.end().printable().c_str());
			}

			// FIXME: if range is assign, have some sort of semaphore for outstanding assignments so we don't assign
			// a ton ranges at once and blow up FDB with reading initial snapshots.
			bmData->addActor.send(doRangeAssignment(bmData, assignment, workerId, seqNo));
		} else {
			// Revoking a range could be a large range that contains multiple ranges.
			auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
			for (auto& it : currentAssignments) {
				// ensure range doesn't truncate existing ranges
				ASSERT(it.begin() >= assignment.keyRange.begin);
				ASSERT(it.end() <= assignment.keyRange.end);

				// It is fine for multiple disjoint sub-ranges to have the same sequence number since they were part of
				// the same logical change

				if (bmData->workerStats.count(it.value())) {
					bmData->workerStats[it.value()].numGranulesAssigned -= 1;
				}

				// revoke the range for the worker that owns it, not the worker specified in the revoke
				bmData->addActor.send(doRangeAssignment(bmData, assignment, it.value(), seqNo));
			}

			bmData->workerAssignments.insert(assignment.keyRange, UID());
			printf("current ranges after inserting revoke: \n");
			for (auto it : bmData->workerAssignments.ranges()) {
				printf("[%s - %s]\n", it.begin().printable().c_str(), it.end().printable().c_str());
			}
		}
	}
}

ACTOR Future<Void> checkManagerLock(Reference<ReadYourWritesTransaction> tr, BlobManagerData* bmData) {
	Optional<Value> currentLockValue = wait(tr->get(blobManagerEpochKey));
	ASSERT(currentLockValue.present());
	int64_t currentEpoch = decodeBlobManagerEpochValue(currentLockValue.get());
	if (currentEpoch != bmData->epoch) {
		ASSERT(currentEpoch > bmData->epoch);

		if (BM_DEBUG) {
			printf("BM %s found new epoch %d > %d in lock check\n",
			       bmData->id.toString().c_str(),
			       currentEpoch,
			       bmData->epoch);
		}
		if (bmData->iAmReplaced.canBeSet()) {
			bmData->iAmReplaced.send(Void());
		}

		// TODO different error?
		throw granule_assignment_conflict();
	}
	tr->addReadConflictRange(singleKeyRange(blobManagerEpochKey));

	return Void();
}

// FIXME: this does all logic in one transaction. Adding a giant range to an existing database to hybridize would spread
// require doing a ton of storage metrics calls, which we should split up across multiple transactions likely.
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

					RangeAssignment ra;
					ra.isAssign = false;
					ra.keyRange = range;
					ra.revoke = RangeRevokeData(true); // dispose=true
					bmData->rangesToAssign.send(ra);
				}

				state std::vector<Future<Standalone<VectorRef<KeyRef>>>> splitFutures;
				// Divide new ranges up into equal chunks by using SS byte sample
				for (KeyRangeRef range : rangesToAdd) {
					// assert that this range contains no currently assigned ranges in this
					splitFutures.push_back(splitRange(tr, range));
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

						RangeAssignment ra;
						ra.isAssign = true;
						ra.keyRange = range;
						ra.assign = RangeAssignmentData(false); // continue=false
						bmData->rangesToAssign.send(ra);
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

static Key granuleLockKey(KeyRange granuleRange) {
	Tuple k;
	k.append(granuleRange.begin).append(granuleRange.end);
	return k.getDataAsStandalone().withPrefix(blobGranuleLockKeys.begin);
}

// FIXME: propagate errors here
ACTOR Future<Void> maybeSplitRange(BlobManagerData* bmData, UID currentWorkerId, KeyRange range) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state Standalone<VectorRef<KeyRef>> newRanges;
	state int64_t newLockSeqno = -1;

	// first get ranges to split
	loop {
		try {
			// redo split if previous txn try failed to calculate it
			if (newRanges.empty()) {
				Standalone<VectorRef<KeyRef>> _newRanges = wait(splitRange(tr, range));
				newRanges = _newRanges;
			}
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	if (newRanges.size() == 2) {
		// not large enough to split, just reassign back to worker
		if (BM_DEBUG) {
			printf("Not splitting existing range [%s - %s). Continuing assignment to %s\n",
			       range.begin.printable().c_str(),
			       range.end.printable().c_str(),
			       currentWorkerId.toString().c_str());
		}
		RangeAssignment raContinue;
		raContinue.isAssign = true;
		raContinue.worker = currentWorkerId;
		raContinue.keyRange = range;
		raContinue.assign = RangeAssignmentData(true); // continue assignment and re-snapshot
		bmData->rangesToAssign.send(raContinue);
		return Void();
	}

	// Need to split range. Persist intent to split and split metadata to DB BEFORE sending split requests
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::Option::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::Option::ACCESS_SYSTEM_KEYS);
			ASSERT(newRanges.size() >= 2);

			// make sure we're still manager when this transaction gets committed
			wait(checkManagerLock(tr, bmData));

			// acquire lock for old granule to make sure nobody else modifies it
			state Key lockKey = granuleLockKey(range);
			Optional<Value> lockValue = wait(tr->get(lockKey));
			ASSERT(lockValue.present());
			std::tuple<int64_t, int64_t, UID> prevGranuleLock = decodeBlobGranuleLockValue(lockValue.get());
			if (std::get<0>(prevGranuleLock) > bmData->epoch) {
				if (BM_DEBUG) {
					printf("BM %s found a higher epoch %d than %d for granule lock of [%s - %s)\n",
					       bmData->id.toString().c_str(),
					       std::get<0>(prevGranuleLock),
					       bmData->epoch,
					       range.begin.printable().c_str(),
					       range.end.printable().c_str());
				}

				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}
			if (newLockSeqno == -1) {
				newLockSeqno = bmData->seqNo;
				bmData->seqNo++;
				ASSERT(newLockSeqno > std::get<1>(prevGranuleLock));
			} else {
				// previous transaction could have succeeded but got commit_unknown_result
				ASSERT(newLockSeqno >= std::get<1>(prevGranuleLock));
			}

			// acquire granule lock so nobody else can make changes to this granule.
			tr->set(lockKey, blobGranuleLockValueFor(bmData->epoch, newLockSeqno, std::get<2>(prevGranuleLock)));

			Standalone<VectorRef<KeyRangeRef>> history;
			history.push_back(history.arena(), range);
			Value historyValue = blobGranuleHistoryValueFor(history);
			// set up split metadata
			for (int i = 0; i < newRanges.size() - 1; i++) {
				Tuple splitKey;
				splitKey.append(range.begin).append(range.end).append(newRanges[i]);
				tr->atomicOp(splitKey.getDataAsStandalone().withPrefix(blobGranuleSplitKeys.begin),
				             blobGranuleSplitValueFor(BlobGranuleSplitState::Started),
				             MutationRef::SetVersionstampedValue);

				Tuple historyKey;
				historyKey.append(newRanges[i]).append(newRanges[i + 1]);
				tr->set(historyKey.getDataAsStandalone().withPrefix(blobGranuleHistoryKeys.begin), historyValue);
			}

			wait(tr->commit());
			break;
		} catch (Error& e) {
			if (e.code() == error_code_granule_assignment_conflict) {
				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}
			wait(tr->onError(e));
		}
	}

	if (BM_DEBUG) {
		printf("Splitting range [%s - %s) into (%d):\n",
		       range.begin.printable().c_str(),
		       range.end.printable().c_str(),
		       newRanges.size() - 1);
		for (int i = 0; i < newRanges.size() - 1; i++) {
			printf("  [%s - %s)\n", newRanges[i].printable().c_str(), newRanges[i + 1].printable().c_str());
		}
	}

	// transaction committed, send range assignments
	// revoke from current worker
	RangeAssignment raRevoke;
	raRevoke.isAssign = false;
	raRevoke.worker = currentWorkerId;
	raRevoke.keyRange = range;
	raRevoke.revoke = RangeRevokeData(false); // not a dispose
	bmData->rangesToAssign.send(raRevoke);

	for (int i = 0; i < newRanges.size() - 1; i++) {
		// reassign new range and do handover of previous range
		RangeAssignment raAssignSplit;
		raAssignSplit.isAssign = true;
		raAssignSplit.keyRange = KeyRangeRef(newRanges[i], newRanges[i + 1]);
		raAssignSplit.assign = RangeAssignmentData(false);
		// don't care who this range gets assigned to
		bmData->rangesToAssign.send(raAssignSplit);
	}

	return Void();
}

void reassignRanges(BlobManagerData* bmData, UID bwId) {
	printf("taking back ranges for worker %s\n", bwId.toString().c_str());
	// for every range owned by this blob worker, we want to
	// - send a revoke request for that range to the blob worker
	// - add the range back to the stream of ranges to be assigned
	for (auto& it : bmData->workerAssignments.ranges()) {
		if (it.cvalue() == bwId) {
			// Send revoke request to worker
			RangeAssignment raRevoke;
			raRevoke.isAssign = false;
			raRevoke.worker = bwId;
			raRevoke.keyRange = it.range();
			raRevoke.revoke = RangeRevokeData(false);
			bmData->rangesToAssign.send(raRevoke);

			// Add range back into the stream of ranges to be assigned
			RangeAssignment raAssign;
			raAssign.isAssign = true;
			raAssign.worker = Optional<UID>();
			raAssign.keyRange = it.range();
			raAssign.assign = RangeAssignmentData(false); // not a continue
			bmData->rangesToAssign.send(raAssign);
		}
	}
}

void killBlobWorker(BlobManagerData* bmData, BlobWorkerInterface bwInterf) {
	UID bwId = bwInterf.id();

	// Remove blob worker from stats map so that when we try to find a worker to takeover the range,
	// the one we just killed isn't considered.
	// Remove it from workersById also since otherwise that addr will remain excluded
	// when we try to recruit new blob workers.
	printf("removing bw %s from BM workerStats\n", bwId.toString().c_str());
	bmData->workerStats.erase(bwId);
	bmData->workersById.erase(bwId);

	// for every range owned by this blob worker, we want to
	// - send a revoke request for that range to the blob worker
	// - add the range back to the stream of ranges to be assigned
	printf("taking back ranges from bw %s\n", bwId.toString().c_str());
	for (auto& it : bmData->workerAssignments.ranges()) {
		if (it.cvalue() == bwId) {
			// Send revoke request to worker
			RangeAssignment raRevoke;
			raRevoke.isAssign = false;
			raRevoke.worker = bwId;
			raRevoke.keyRange = it.range();
			raRevoke.revoke = RangeRevokeData(false);
			bmData->rangesToAssign.send(raRevoke);

			// Add range back into the stream of ranges to be assigned
			RangeAssignment raAssign;
			raAssign.isAssign = true;
			raAssign.worker = Optional<UID>();
			raAssign.keyRange = it.range();
			raAssign.assign = RangeAssignmentData(false); // not a continue
			bmData->rangesToAssign.send(raAssign);
		}
	}

	// Send halt to blob worker, with no expectation of hearing back
	printf("sending halt to bw %s\n", bwId.toString().c_str());
	bmData->addActor.send(
	    brokenPromiseToNever(bwInterf.haltBlobWorker.getReply(HaltBlobWorkerRequest(bmData->epoch, bmData->id))));
}

ACTOR Future<Void> monitorBlobWorkerStatus(BlobManagerData* bmData, BlobWorkerInterface bwInterf) {
	state KeyRangeMap<std::pair<int64_t, int64_t>> lastSeenSeqno;
	// outer loop handles reconstructing stream if it got a retryable error
	loop {
		try {
			state ReplyPromiseStream<GranuleStatusReply> statusStream =
			    bwInterf.granuleStatusStreamRequest.getReplyStream(GranuleStatusStreamRequest(bmData->epoch));
			// read from stream until worker fails (should never get explicit end_of_stream)
			loop {
				GranuleStatusReply rep = waitNext(statusStream.getFuture());

				if (BM_DEBUG) {
					printf("BM %lld got status of [%s - %s) @ (%lld, %lld) from BW %s: %s\n",
					       bmData->epoch,
					       rep.granuleRange.begin.printable().c_str(),
					       rep.granuleRange.end.printable().c_str(),
					       rep.epoch,
					       rep.seqno,
					       bwInterf.id().toString().c_str(),
					       rep.doSplit ? "split" : "");
				}
				if (rep.epoch > bmData->epoch) {
					if (BM_DEBUG) {
						printf("BM heard from BW %s that there is a new manager with higher epoch\n",
						       bwInterf.id().toString().c_str());
					}
					if (bmData->iAmReplaced.canBeSet()) {
						bmData->iAmReplaced.send(Void());
					}
				}

				// TODO maybe this won't be true eventually, but right now the only time the blob worker reports back is
				// to split the range.
				ASSERT(rep.doSplit);

				auto currGranuleAssignment = bmData->workerAssignments.rangeContaining(rep.granuleRange.begin);
				if (!(currGranuleAssignment.begin() == rep.granuleRange.begin &&
				      currGranuleAssignment.end() == rep.granuleRange.end &&
				      currGranuleAssignment.cvalue() == bwInterf.id())) {
					continue;
				}

				auto lastReqForGranule = lastSeenSeqno.rangeContaining(rep.granuleRange.begin);
				if (rep.granuleRange.begin == lastReqForGranule.begin() &&
				    rep.granuleRange.end == lastReqForGranule.end() && rep.epoch == lastReqForGranule.value().first &&
				    rep.seqno == lastReqForGranule.value().second) {
					if (BM_DEBUG) {
						printf("Manager %lld received repeat status for the same granule [%s - %s) @ %lld, ignoring.",
						       bmData->epoch,
						       rep.granuleRange.begin.printable().c_str(),
						       rep.granuleRange.end.printable().c_str());
					}
				} else {
					if (BM_DEBUG) {
						printf("Manager %lld evaluating [%s - %s) for split\n",
						       bmData->epoch,
						       rep.granuleRange.begin.printable().c_str(),
						       rep.granuleRange.end.printable().c_str());
					}
					lastSeenSeqno.insert(rep.granuleRange, std::pair(rep.epoch, rep.seqno));
					bmData->addActor.send(maybeSplitRange(bmData, bwInterf.id(), rep.granuleRange));
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}
			// if we got an error constructing or reading from stream that is retryable, wait and retry.
			ASSERT(e.code() != error_code_end_of_stream);
			if (e.code() == error_code_connection_failed || e.code() == error_code_request_maybe_delivered) {
				// FIXME: this could throw connection_failed and we could handle catch this the same as the failure
				// detection triggering
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				continue;
			} else {
				if (BM_DEBUG) {
					printf("BM got unexpected error %s monitoring BW %s status\n",
					       e.name(),
					       bwInterf.id().toString().c_str());
				}
				// TODO change back from SevError?
				TraceEvent(SevError, "BWStatusMonitoringFailed", bmData->id)
				    .detail("BlobWorkerID", bwInterf.id())
				    .error(e);
				throw e;
			}
		}
	}
}

ACTOR Future<Void> monitorBlobWorker(BlobManagerData* bmData, BlobWorkerInterface bwInterf) {
	try {
		state Future<Void> waitFailure = waitFailureClient(bwInterf.waitFailure, SERVER_KNOBS->BLOB_WORKER_TIMEOUT);
		state Future<Void> monitorStatus = monitorBlobWorkerStatus(bmData, bwInterf);

		choose {
			when(wait(waitFailure)) {
				// FIXME: actually handle this!!
				if (BM_DEBUG) {
					printf("BM %lld detected BW %s is dead\n", bmData->epoch, bwInterf.id().toString().c_str());
				}
				TraceEvent("BlobWorkerFailed", bmData->id).detail("BlobWorkerID", bwInterf.id());
				killBlobWorker(bmData, bwInterf);
				return Void();
			}
			when(wait(monitorStatus)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		// FIXME: forward errors somewhere from here
		if (BM_DEBUG) {
			printf("BM got unexpected error %s monitoring BW %s\n", e.name(), bwInterf.id().toString().c_str());
		}
		// TODO change back from SevError?
		TraceEvent(SevError, "BWMonitoringFailed", bmData->id).detail("BlobWorkerID", bwInterf.id()).error(e);
		throw e;
	}

	// Trigger recruitment for a new blob worker
	printf("restarting recruitment in monitorblobworker\n");
	bmData->restartRecruiting.trigger();

	printf("about to stop monitoring %s\n", bwInterf.id().toString().c_str());
	return Void();
}

// TODO this is only for chaos testing right now!! REMOVE LATER
ACTOR Future<Void> rangeMover(BlobManagerData* bmData) {
	ASSERT(g_network->isSimulated());
	loop {
		wait(delay(30.0));

		if (g_simulator.speedUpSimulation) {
			if (BM_DEBUG) {
				printf("Range mover stopping\n");
			}
			return Void();
		}

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

					// FIXME: with low probability, could immediately revoke it from the new assignment and move it back
					// right after to test that race

					RangeAssignment revokeOld;
					revokeOld.isAssign = false;
					revokeOld.keyRange = randomRange.range();
					revokeOld.worker = randomRange.value();
					revokeOld.revoke = RangeRevokeData(false);
					bmData->rangesToAssign.send(revokeOld);

					RangeAssignment assignNew;
					assignNew.isAssign = true;
					assignNew.keyRange = randomRange.range();
					assignNew.assign = RangeAssignmentData(false); // not a continue
					bmData->rangesToAssign.send(assignNew);
					break;
				}
			}
			if (tries == 0 && BM_DEBUG) {
				printf("Range mover couldn't find random range to move, skipping\n");
			}
		} else if (BM_DEBUG) {
			printf("Range mover found %d workers, skipping\n", bmData->workerAssignments.size());
		}
	}
}

// Returns the number of blob workers on addr
int numExistingBWOnAddr(BlobManagerData* self, const AddressExclusion& addr) {
	int numExistingBW = 0;
	for (auto& server : self->workersById) {
		const NetworkAddress& netAddr = server.second.stableAddress();
		AddressExclusion usedAddr(netAddr.ip, netAddr.port);
		if (usedAddr == addr) {
			++numExistingBW;
		}
	}

	return numExistingBW;
}

// Tries to recruit a blob worker on the candidateWorker process
ACTOR Future<Void> initializeBlobWorker(BlobManagerData* self, RecruitBlobWorkerReply candidateWorker) {
	const NetworkAddress& netAddr = candidateWorker.worker.stableAddress();
	AddressExclusion workerAddr(netAddr.ip, netAddr.port);
	// Ask the candidateWorker to initialize a BW only if the worker does not have a pending request
	if (numExistingBWOnAddr(self, workerAddr) == 0 &&
	    self->recruitingLocalities.count(candidateWorker.worker.stableAddress()) == 0) {
		state UID interfaceId = deterministicRandom()->randomUniqueID();

		state InitializeBlobWorkerRequest initReq;
		initReq.reqId = deterministicRandom()->randomUniqueID();
		initReq.interfaceId = interfaceId;

		// acknowledge that this worker is currently being recruited on
		self->recruitingLocalities.insert(candidateWorker.worker.stableAddress());

		TraceEvent("BMRecruiting")
		    .detail("State", "Sending request to worker")
		    .detail("WorkerID", candidateWorker.worker.id())
		    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
		    .detail("Interf", interfaceId)
		    .detail("Addr", candidateWorker.worker.address());

		// send initialization request to worker (i.e. worker.actor.cpp)
		// here, the worker will construct the blob worker at which point the BW will start!
		Future<ErrorOr<InitializeBlobWorkerReply>> fRecruit =
		    candidateWorker.worker.blobWorker.tryGetReply(initReq, TaskPriority::BlobManager);

		// wait on the reply to the request
		state ErrorOr<InitializeBlobWorkerReply> newBlobWorker = wait(fRecruit);

		// if the initialization failed in an unexpected way, then kill the BM.
		// if it failed in an expected way, add some delay before we try to recruit again
		// on this worker
		if (newBlobWorker.isError()) {
			TraceEvent(SevWarn, "BMRecruitmentError").error(newBlobWorker.getError());
			if (!newBlobWorker.isError(error_code_recruitment_failed) &&
			    !newBlobWorker.isError(error_code_request_maybe_delivered)) {
				throw newBlobWorker.getError();
			}
			wait(delay(SERVER_KNOBS->STORAGE_RECRUITMENT_DELAY, TaskPriority::BlobManager));
		}

		// if the initialization succeeded, add the blob worker's interface to
		// the blob manager's data and start monitoring the blob worker
		if (newBlobWorker.present()) {
			BlobWorkerInterface bwi = newBlobWorker.get().interf;

			self->workersById[bwi.id()] = bwi;
			self->workerStats[bwi.id()] = BlobWorkerStats();
			self->addActor.send(monitorBlobWorker(self, bwi));

			TraceEvent("BMRecruiting")
			    .detail("State", "Finished request")
			    .detail("WorkerID", candidateWorker.worker.id())
			    .detail("WorkerLocality", candidateWorker.worker.locality.toString())
			    .detail("Interf", interfaceId)
			    .detail("Addr", candidateWorker.worker.address());
		}

		// acknowledge that this worker is not actively being recruited on anymore.
		// if the initialization did succeed, then this worker will still be excluded
		// since it was added to workersById.
		self->recruitingLocalities.erase(candidateWorker.worker.stableAddress());
	}

	// try to recruit more blob workers
	self->restartRecruiting.trigger();
	return Void();
}

// Recruits blob workers in a loop
ACTOR Future<Void> blobWorkerRecruiter(
    BlobManagerData* self,
    Reference<IAsyncListener<RequestStream<RecruitBlobWorkerRequest>>> recruitBlobWorker) {
	state Future<RecruitBlobWorkerReply> fCandidateWorker;
	state RecruitBlobWorkerRequest lastRequest;

	loop {
		try {
			state RecruitBlobWorkerRequest recruitReq;

			// workers that are used by existing blob workers should be excluded
			for (auto const& [bwId, bwInterf] : self->workersById) {
				auto addr = bwInterf.stableAddress();
				AddressExclusion addrExcl(addr.ip, addr.port);
				recruitReq.excludeAddresses.emplace_back(addrExcl);
			}

			// workers that are used by blob workers that are currently being recruited should be excluded
			for (auto addr : self->recruitingLocalities) {
				recruitReq.excludeAddresses.emplace_back(AddressExclusion(addr.ip, addr.port));
			}

			TraceEvent("BMRecruiting").detail("State", "Sending request to CC");
			/*
			printf("EXCLUDING THE FOLLOWING IN REQ:\n");
			for (auto addr : recruitReq.excludeAddresses) {
			    printf("- %s\n", addr.toString().c_str());
			}
			*/

			if (!fCandidateWorker.isValid() || fCandidateWorker.isReady() ||
			    recruitReq.excludeAddresses != lastRequest.excludeAddresses) {
				lastRequest = recruitReq;
				// send req to cluster controller to get back a candidate worker we can recruit on
				fCandidateWorker =
				    brokenPromiseToNever(recruitBlobWorker->get().getReply(recruitReq, TaskPriority::BlobManager));
			}

			choose {
				// when we get back a worker we can use, we will try to initialize a blob worker onto that process
				when(RecruitBlobWorkerReply candidateWorker = wait(fCandidateWorker)) {
					self->addActor.send(initializeBlobWorker(self, candidateWorker));
				}

				// when the CC changes, so does the request stream so we need to restart recruiting here
				when(wait(recruitBlobWorker->onChange())) { fCandidateWorker = Future<RecruitBlobWorkerReply>(); }

				// signal used to restart the loop and try to recruit the next blob worker
				when(wait(self->restartRecruiting.onTrigger())) { printf("RESTARTED RECRUITING. BACK TO TOP\n"); }
			}
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY, TaskPriority::BlobManager));
		} catch (Error& e) {
			if (e.code() != error_code_timed_out) {
				throw;
			}
			TEST(true); // Blob worker recruitment timed out
		}
	}
}

ACTOR Future<Void> blobManager(BlobManagerInterface bmInterf,
                               Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                               int64_t epoch) {
	state BlobManagerData self(deterministicRandom()->randomUniqueID(),
	                           openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True));

	state Future<Void> collection = actorCollection(self.addActor.getFuture());

	if (BM_DEBUG) {
		printf("Blob manager starting...\n");
	}

	self.epoch = epoch;

	// make sure the epoch hasn't gotten stale
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self.db);

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	try {
		wait(checkManagerLock(tr, &self));
	} catch (Error& e) {
		if (BM_DEBUG) {
			printf("Blob manager lock check got unexpected error %s. Dying...\n", e.name());
		}
		return Void();
	}

	if (BM_DEBUG) {
		printf("Blob manager acquired lock at epoch %lld\n", epoch);
	}

	// needed to pick up changes to dbinfo in case new CC comes along
	auto recruitBlobWorker = IAsyncListener<RequestStream<RecruitBlobWorkerRequest>>::create(
	    dbInfo, [](auto const& info) { return info.clusterInterface.recruitBlobWorker; });

	self.addActor.send(blobWorkerRecruiter(&self, recruitBlobWorker));
	self.addActor.send(monitorClientRanges(&self));
	self.addActor.send(rangeAssigner(&self));

	if (BUGGIFY) {
		self.addActor.send(rangeMover(&self));
	}

	// TODO probably other things here eventually
	try {
		loop choose {
			when(wait(self.iAmReplaced.getFuture())) {
				if (BM_DEBUG) {
					printf("Blob Manager exiting because it is replaced\n");
				}
				break;
			}
			when(HaltBlobManagerRequest req = waitNext(bmInterf.haltBlobManager.getFuture())) {
				req.reply.send(Void());
				TraceEvent("BlobManagerHalted", bmInterf.id()).detail("ReqID", req.requesterID);
				break;
			}
			when(wait(collection)) {
				TraceEvent("BlobManagerActorCollectionError");
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& err) {
		TraceEvent("BlobManagerDied", bmInterf.id()).error(err, true);
	}
	return Void();
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
// DB has [B - C). It should show up in knownBlobRanges, [B - C) should be in added, and [A - B) and [C - D) should
// be in removed.
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
