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

#include <queue>
#include <vector>
#include <unordered_map>

#include "contrib/fmt-8.0.1/include/fmt/format.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BlobManagerInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

#define BM_DEBUG true

// FIXME: change all BlobManagerData* to Reference<BlobManagerData> to avoid segfaults if core loop gets error

// TODO add comments + documentation
void handleClientBlobRange(KeyRangeMap<bool>* knownBlobRanges,
                           Arena& ar,
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
                            Arena& ar,
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
	AssignRequestType type;

	RangeAssignmentData() : type(AssignRequestType::Normal) {}
	RangeAssignmentData(AssignRequestType type) : type(type) {}
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
	Optional<Key> dcId;
	PromiseStream<Future<Void>> addActor;

	Reference<BackupContainerFileSystem> bstore;

	std::unordered_map<UID, BlobWorkerInterface> workersById;
	std::unordered_map<UID, BlobWorkerStats> workerStats; // mapping between workerID -> workerStats
	std::unordered_set<NetworkAddress> workerAddresses;
	std::unordered_set<UID> deadWorkers;
	KeyRangeMap<UID> workerAssignments;
	KeyRangeMap<bool> knownBlobRanges;

	AsyncTrigger startRecruiting;
	Debouncer restartRecruiting;
	std::set<NetworkAddress> recruitingLocalities; // the addrs of the workers being recruited on
	AsyncVar<int> recruitingStream;

	int64_t epoch = -1;
	int64_t seqNo = 1;

	Promise<Void> iAmReplaced;

	// The order maintained here is important. The order ranges are put into the promise stream is the order they get
	// assigned sequence numbers
	PromiseStream<RangeAssignment> rangesToAssign;

	BlobManagerData(UID id, Database db, Optional<Key> dcId)
	  : id(id), db(db), dcId(dcId), knownBlobRanges(false, normalKeys.end),
	    restartRecruiting(SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY), recruitingStream(0) {}
	~BlobManagerData() { printf("Destroying blob manager data for %s\n", id.toString().c_str()); }
};

ACTOR Future<Standalone<VectorRef<KeyRef>>> splitRange(Reference<ReadYourWritesTransaction> tr, KeyRange range) {
	// TODO is it better to just pass empty metrics to estimated?
	// redo split if previous txn failed to calculate it
	loop {
		try {
			if (BM_DEBUG) {
				printf(
				    "Splitting new range [%s - %s)\n", range.begin.printable().c_str(), range.end.printable().c_str());
			}
			StorageMetrics estimated = wait(tr->getTransaction().getStorageMetrics(range, CLIENT_KNOBS->TOO_MANY));

			if (BM_DEBUG) {
				fmt::print("Estimated bytes for [{0} - {1}): {2}\n",
				           range.begin.printable(),
				           range.end.printable(),
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
				ASSERT(keys.size() >= 2);
				return keys;
			} else {
				// printf("  Not splitting range\n");
				Standalone<VectorRef<KeyRef>> keys;
				keys.push_back_deep(keys.arena(), range.begin);
				keys.push_back_deep(keys.arena(), range.end);
				return keys;
			}
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Picks a worker with the fewest number of already assigned ranges.
// If there is a tie, picks one such worker at random.
ACTOR Future<UID> pickWorkerForAssign(BlobManagerData* bmData) {
	// wait until there are BWs to pick from
	while (bmData->workerStats.size() == 0) {
		bmData->restartRecruiting.trigger();
		wait(bmData->recruitingStream.onChange());
	}

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
		fmt::print("BM {0} {1} range [{2} - {3}) @ ({4}, {5})\n",
		           bmData->id.toString(),
		           assignment.isAssign ? "assigning" : "revoking",
		           assignment.keyRange.begin.printable(),
		           assignment.keyRange.end.printable(),
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
			req.type = assignment.assign.get().type;

			// if that worker isn't alive anymore, add the range back into the stream
			if (bmData->workersById.count(workerID) == 0) {
				throw no_more_servers();
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
		if (e.code() == error_code_operation_cancelled) {
			throw;
		}
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
				printf("BM got error revoking range [%s - %s) from worker",
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
		state RangeAssignment assignment = waitNext(bmData->rangesToAssign.getFuture());
		state int64_t seqNo = bmData->seqNo;
		bmData->seqNo++;

		// modify the in-memory assignment data structures, and send request off to worker
		state UID workerId;
		if (assignment.isAssign) {
			// Ensure range isn't currently assigned anywhere, and there is only 1 intersecting range
			auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
			int count = 0;
			for (auto i = currentAssignments.begin(); i != currentAssignments.end(); ++i) {
				/* TODO: rethink asserts here
				if (assignment.assign.get().type == AssignRequestType::Continue) {
				    ASSERT(assignment.worker.present());
				    ASSERT(it.value() == assignment.worker.get());
				} else {
				    ASSERT(it.value() == UID());
				}
				*/
				count++;
			}
			ASSERT(count == 1);

			if (assignment.worker.present() && assignment.worker.get().isValid()) {
				workerId = assignment.worker.get();
			} else {
				if (BM_DEBUG) {
					printf("About to pick worker for seqno %d in BM %s\n", seqNo, bmData->id.toString().c_str());
				}
				UID _workerId = wait(pickWorkerForAssign(bmData));
				if (BM_DEBUG) {
					printf("Found worker BW %s for seqno %d\n", _workerId.toString().c_str(), seqNo);
				}
				workerId = _workerId;
			}
			bmData->workerAssignments.insert(assignment.keyRange, workerId);

			// If we know about the worker and this is not a continue, then this is a new range for the worker
			if (bmData->workerStats.count(workerId) && assignment.assign.get().type != AssignRequestType::Continue) {
				bmData->workerStats[workerId].numGranulesAssigned += 1;
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
			fmt::print(
			    "BM {0} found new epoch {1} > {2} in lock check\n", bmData->id.toString(), currentEpoch, bmData->epoch);
		}
		if (bmData->iAmReplaced.canBeSet()) {
			bmData->iAmReplaced.send(Void());
		}

		throw granule_assignment_conflict();
	}
	tr->addReadConflictRange(singleKeyRange(blobManagerEpochKey));

	return Void();
}

// FIXME: this does all logic in one transaction. Adding a giant range to an existing database to blobify would
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
						// only add the client range if this is the first BM or it's not already assigned
						if (bmData->epoch == 1 || bmData->workerAssignments.intersectingRanges(range).empty()) {
							if (BM_DEBUG) {
								printf(
								    "    [%s - %s)\n", range.begin.printable().c_str(), range.end.printable().c_str());
							}

							RangeAssignment ra;
							ra.isAssign = true;
							ra.keyRange = range;
							ra.assign = RangeAssignmentData(); // type=normal
							bmData->rangesToAssign.send(ra);
						}
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

ACTOR Future<Void> maybeSplitRange(BlobManagerData* bmData,
                                   UID currentWorkerId,
                                   KeyRange granuleRange,
                                   UID granuleID,
                                   Version granuleStartVersion,
                                   Version latestVersion) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state Standalone<VectorRef<KeyRef>> newRanges;
	state int64_t newLockSeqno = -1;

	// first get ranges to split
	Standalone<VectorRef<KeyRef>> _newRanges = wait(splitRange(tr, granuleRange));
	newRanges = _newRanges;

	if (newRanges.size() == 2) {
		// not large enough to split, just reassign back to worker
		if (BM_DEBUG) {
			printf("Not splitting existing range [%s - %s). Continuing assignment to %s\n",
			       granuleRange.begin.printable().c_str(),
			       granuleRange.end.printable().c_str(),
			       currentWorkerId.toString().c_str());
		}
		RangeAssignment raContinue;
		raContinue.isAssign = true;
		raContinue.worker = currentWorkerId;
		raContinue.keyRange = granuleRange;
		raContinue.assign = RangeAssignmentData(AssignRequestType::Continue); // continue assignment and re-snapshot
		bmData->rangesToAssign.send(raContinue);
		return Void();
	}

	if (BM_DEBUG) {
		printf("Splitting range [%s - %s) into (%d):\n",
		       granuleRange.begin.printable().c_str(),
		       granuleRange.end.printable().c_str(),
		       newRanges.size() - 1);
		for (int i = 0; i < newRanges.size(); i++) {
			printf("    %s\n", newRanges[i].printable().c_str());
		}
	}

	// Need to split range. Persist intent to split and split metadata to DB BEFORE sending split requests
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::Option::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::Option::ACCESS_SYSTEM_KEYS);
			ASSERT(newRanges.size() > 2);

			// make sure we're still manager when this transaction gets committed
			wait(checkManagerLock(tr, bmData));

			// acquire lock for old granule to make sure nobody else modifies it
			state Key lockKey = blobGranuleLockKeyFor(granuleRange);
			Optional<Value> lockValue = wait(tr->get(lockKey));
			ASSERT(lockValue.present());
			std::tuple<int64_t, int64_t, UID> prevGranuleLock = decodeBlobGranuleLockValue(lockValue.get());
			if (std::get<0>(prevGranuleLock) > bmData->epoch) {
				if (BM_DEBUG) {
					fmt::print("BM {0} found a higher epoch {1} than {2} for granule lock of [{3} - {4})\n",
					           bmData->id.toString(),
					           std::get<0>(prevGranuleLock),
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
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

			// set up split metadata
			for (int i = 0; i < newRanges.size() - 1; i++) {
				UID newGranuleID = deterministicRandom()->randomUniqueID();

				Key splitKey = blobGranuleSplitKeyFor(granuleID, newGranuleID);

				tr->atomicOp(splitKey,
				             blobGranuleSplitValueFor(BlobGranuleSplitState::Initialized),
				             MutationRef::SetVersionstampedValue);

				Key historyKey = blobGranuleHistoryKeyFor(KeyRangeRef(newRanges[i], newRanges[i + 1]), latestVersion);

				Standalone<BlobGranuleHistoryValue> historyValue;
				historyValue.granuleID = newGranuleID;
				historyValue.parentGranules.push_back(historyValue.arena(),
				                                      std::pair(granuleRange, granuleStartVersion));

				/*printf("Creating history entry [%s - %s) - [%lld - %lld)\n",
				       newRanges[i].printable().c_str(),
				       newRanges[i + 1].printable().c_str(),
				       granuleStartVersion,
				       latestVersion);*/
				tr->set(historyKey, blobGranuleHistoryValueFor(historyValue));
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

	// transaction committed, send range assignments
	// revoke from current worker
	RangeAssignment raRevoke;
	raRevoke.isAssign = false;
	raRevoke.worker = currentWorkerId;
	raRevoke.keyRange = granuleRange;
	raRevoke.revoke = RangeRevokeData(false); // not a dispose
	bmData->rangesToAssign.send(raRevoke);

	for (int i = 0; i < newRanges.size() - 1; i++) {
		// reassign new range and do handover of previous range
		RangeAssignment raAssignSplit;
		raAssignSplit.isAssign = true;
		raAssignSplit.keyRange = KeyRangeRef(newRanges[i], newRanges[i + 1]);
		raAssignSplit.assign = RangeAssignmentData();
		// don't care who this range gets assigned to
		bmData->rangesToAssign.send(raAssignSplit);
	}

	return Void();
}

ACTOR Future<Void> deregisterBlobWorker(BlobManagerData* bmData, BlobWorkerInterface interf) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		try {
			Key blobWorkerListKey = blobWorkerListKeyFor(interf.id());
			tr->addReadConflictRange(singleKeyRange(blobWorkerListKey));
			tr->clear(blobWorkerListKey);

			wait(tr->commit());

			if (BM_DEBUG) {
				printf("Deregistered blob worker %s\n", interf.id().toString().c_str());
			}
			return Void();
		} catch (Error& e) {
			if (BM_DEBUG) {
				printf("Deregistering blob worker %s got error %s\n", interf.id().toString().c_str(), e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> killBlobWorker(BlobManagerData* bmData, BlobWorkerInterface bwInterf, bool registered) {
	state UID bwId = bwInterf.id();

	// Remove blob worker from stats map so that when we try to find a worker to takeover the range,
	// the one we just killed isn't considered.
	// Remove it from workersById also since otherwise that worker addr will remain excluded
	// when we try to recruit new blob workers.

	if (registered) {
		bmData->deadWorkers.insert(bwId);
		bmData->workerStats.erase(bwId);
		bmData->workersById.erase(bwId);
		bmData->workerAddresses.erase(bwInterf.stableAddress());
	}

	// Remove blob worker from persisted list of blob workers
	Future<Void> deregister = deregisterBlobWorker(bmData, bwInterf);

	// restart recruiting to replace the dead blob worker
	bmData->restartRecruiting.trigger();

	// for every range owned by this blob worker, we want to
	// - send a revoke request for that range
	// - add the range back to the stream of ranges to be assigned
	if (BM_DEBUG) {
		printf("Taking back ranges from BW %s\n", bwId.toString().c_str());
	}
	// copy ranges into vector before sending, because send then modifies workerAssignments
	state std::vector<KeyRange> rangesToMove;
	for (auto& it : bmData->workerAssignments.ranges()) {
		if (it.cvalue() == bwId) {
			rangesToMove.push_back(it.range());
		}
	}
	for (auto& it : rangesToMove) {
		// Send revoke request
		RangeAssignment raRevoke;
		raRevoke.isAssign = false;
		raRevoke.keyRange = it;
		raRevoke.revoke = RangeRevokeData(false);
		bmData->rangesToAssign.send(raRevoke);

		// Add range back into the stream of ranges to be assigned
		RangeAssignment raAssign;
		raAssign.isAssign = true;
		raAssign.worker = Optional<UID>();
		raAssign.keyRange = it;
		raAssign.assign = RangeAssignmentData(); // not a continue
		bmData->rangesToAssign.send(raAssign);
	}

	// Send halt to blob worker, with no expectation of hearing back
	if (BM_DEBUG) {
		printf("Sending halt to BW %s\n", bwId.toString().c_str());
	}
	bmData->addActor.send(
	    brokenPromiseToNever(bwInterf.haltBlobWorker.getReply(HaltBlobWorkerRequest(bmData->epoch, bmData->id))));

	wait(deregister);

	if (registered) {
		bmData->deadWorkers.erase(bwId);
	}

	return Void();
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
					fmt::print("BM {0} got status of [{1} - {2}) @ ({3}, {4}) from BW {5}: {6}\n",
					           bmData->epoch,
					           rep.granuleRange.begin.printable(),
					           rep.granuleRange.end.printable(),
					           rep.epoch,
					           rep.seqno,
					           bwInterf.id().toString(),
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
				} else if (rep.epoch < bmData->epoch) {
					// TODO: revoke the range from that worker? and send optimistic halt req to other (zombie) BM?
					// it's optimistic because such a BM is not necessarily a zombie. it could have gotten killed
					// properly but the BW that sent this reply was behind (i.e. it started the req when the old BM
					// was in charge and finished by the time the new BM took over)
					continue;
				}

				// TODO maybe this won't be true eventually, but right now the only time the blob worker reports
				// back is to split the range.
				ASSERT(rep.doSplit);

				// only evaluate for split if this worker currently owns the granule in this blob manager's mapping
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
						fmt::print("Manager {0} received repeat status for the same granule [{1} - {2}), ignoring.",
						           bmData->epoch,
						           rep.granuleRange.begin.printable(),
						           rep.granuleRange.end.printable());
					}
				} else {
					if (BM_DEBUG) {
						fmt::print("Manager {0} evaluating [{1} - {2}) for split\n",
						           bmData->epoch,
						           rep.granuleRange.begin.printable().c_str(),
						           rep.granuleRange.end.printable().c_str());
					}
					lastSeenSeqno.insert(rep.granuleRange, std::pair(rep.epoch, rep.seqno));
					bmData->addActor.send(maybeSplitRange(
					    bmData, bwInterf.id(), rep.granuleRange, rep.granuleID, rep.startVersion, rep.latestVersion));
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}

			// TODO: figure out why waitFailure in monitorBlobWorker doesn't pick up the connection failure first
			if (e.code() == error_code_connection_failed || e.code() == error_code_broken_promise) {
				throw e;
			}

			// if we got an error constructing or reading from stream that is retryable, wait and retry.
			ASSERT(e.code() != error_code_end_of_stream);
			if (e.code() == error_code_request_maybe_delivered) {
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
				if (BM_DEBUG) {
					fmt::print("BM {0} detected BW {1} is dead\n", bmData->epoch, bwInterf.id().toString());
				}
				TraceEvent("BlobWorkerFailed", bmData->id).detail("BlobWorkerID", bwInterf.id());
			}
			when(wait(monitorStatus)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		// will blob worker get cleaned up in this case?
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}

		// TODO: re-evaluate the expected errors here once wait failure issue is resolved
		// Expected errors here are: [connection_failed, broken_promise]
		if (e.code() != error_code_connection_failed && e.code() != error_code_broken_promise) {
			if (BM_DEBUG) {
				printf("BM got unexpected error %s monitoring BW %s\n", e.name(), bwInterf.id().toString().c_str());
			}
			// TODO change back from SevError?
			TraceEvent(SevError, "BWMonitoringFailed", bmData->id).detail("BlobWorkerID", bwInterf.id()).error(e);
			throw e;
		}
	}

	// kill the blob worker
	wait(killBlobWorker(bmData, bwInterf, true));

	if (BM_DEBUG) {
		printf("No longer monitoring BW %s\n", bwInterf.id().toString().c_str());
	}
	return Void();
}

ACTOR Future<Void> checkBlobWorkerList(BlobManagerData* bmData, Promise<Void> workerListReady) {
	loop {
		// Get list of last known blob workers
		// note: the list will include every blob worker that the old manager knew about,
		// but it might also contain blob workers that died while the new manager was being recruited
		std::vector<BlobWorkerInterface> blobWorkers = wait(getBlobWorkers(bmData->db));
		// add all blob workers to this new blob manager's records and start monitoring it
		for (auto& worker : blobWorkers) {
			if (!bmData->deadWorkers.count(worker.id())) {
				if (!bmData->workerAddresses.count(worker.stableAddress()) && worker.locality.dcId() == bmData->dcId) {
					bmData->workerAddresses.insert(worker.stableAddress());
					bmData->workersById[worker.id()] = worker;
					bmData->workerStats[worker.id()] = BlobWorkerStats();
					bmData->addActor.send(monitorBlobWorker(bmData, worker));
				} else if (!bmData->workersById.count(worker.id())) {
					bmData->addActor.send(killBlobWorker(bmData, worker, false));
				}
			}
		}
		if (workerListReady.canBeSet()) {
			workerListReady.send(Void());
		}
		wait(delay(SERVER_KNOBS->BLOB_WORKERLIST_FETCH_INTERVAL));
	}
}

ACTOR Future<Void> recoverBlobManager(BlobManagerData* bmData) {
	state Promise<Void> workerListReady;
	bmData->addActor.send(checkBlobWorkerList(bmData, workerListReady));
	wait(workerListReady.getFuture());

	// Once we acknowledge the existing blob workers, we can go ahead and recruit new ones
	bmData->startRecruiting.trigger();

	// skip them rest of the algorithm for the first blob manager
	if (bmData->epoch == 1) {
		return Void();
	}

	// At this point, bmData->workersById is a list of all alive blob workers, but could also include some dead BWs.
	// The algorithm below works as follows:
	// 1. We get the existing granule mappings that were persisted by blob workers who were assigned ranges and
	//    add them to bmData->granuleAssignments, which is a key range map.
	//    Details: re-assignments might have happened between the time the mapping was last updated and now.
	//    For example, suppose a blob manager sends requests to the range assigner stream to move a granule G.
	//    However, before sending those requests off to the workers, the BM dies. So the persisting mapping
	//    still has G->oldWorker. The following algorithm will re-assign G to oldWorker (as long as it is also still
	//    alive). Note that this is fine because it simply means that the range was not moved optimally, but it is
	//    still owned. In the above case, even if the revoke goes through, since we don't update the mapping during
	//    revokes, this is the same as the case above. Another case to consider is when a blob worker dies when the
	//    BM is recovering. Now the mapping at this time looks like G->deadBW. But the rangeAssigner handles this:
	//    we'll try to assign a range to a dead worker and fail and reassign it to the next best worker.
	//
	// 2. We get the existing split intentions that were Started but not acknowledged by any blob workers and
	//    add them to our key range map, bmData->granuleAssignments. Note that we are adding them on top of
	//    the granule mappings and since we are using a key range map, we end up with the same set of shard
	//    boundaries as the old blob manager had. For these splits, we simply assign the range to the next
	//    best worker. This is not any worst than what the old blob manager would have done.
	//    Details: Note that this means that if a worker we intended to give a splitted range to dies
	//    before the new BM recovers, then we'll simply assign the range to the next best worker.
	//
	// 3. For every range in our granuleAssignments, we send an assign request to the stream of requests,
	//    ultimately giving every range back to some worker (trying to mimic the state of the old BM).
	//    If the worker already had the range, this is a no-op. If the worker didn't have it, it will
	//    begin persisting it. The worker that had the same range before will now be at a lower seqno.

	state KeyRangeMap<Optional<UID>> workerAssignments;
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

	// Step 1. Get the latest known mapping of granules to blob workers (i.e. assignments)
	state KeyRef beginKey = normalKeys.begin;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			wait(checkManagerLock(tr, bmData));

			// TODO: replace row limit with knob
			KeyRange nextRange(KeyRangeRef(beginKey, normalKeys.end));
			RangeResult results = wait(
			    krmGetRanges(tr, blobGranuleMappingKeys.begin, nextRange, 10000, GetRangeLimits::BYTE_LIMIT_UNLIMITED));

			// Add the mappings to our in memory key range map
			for (int rangeIdx = 0; rangeIdx < results.size() - 1; rangeIdx++) {
				Key granuleStartKey = results[rangeIdx].key;
				Key granuleEndKey = results[rangeIdx + 1].key;
				if (results[rangeIdx].value.size()) {
					// note: if the old owner is dead, we handle this in rangeAssigner
					UID existingOwner = decodeBlobGranuleMappingValue(results[rangeIdx].value);
					workerAssignments.insert(KeyRangeRef(granuleStartKey, granuleEndKey), existingOwner);
				}
			}

			if (!results.more) {
				break;
			}

			beginKey = results.readThrough.get();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// Step 2. Get the latest known split intentions
	tr->reset();
	beginKey = blobGranuleSplitKeys.begin;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			wait(checkManagerLock(tr, bmData));

			// TODO: replace row limit with knob
			RangeResult results = wait(tr->getRange(KeyRangeRef(beginKey, blobGranuleSplitKeys.end), 10000));

			// Add the granules for the started split intentions to the in-memory key range map
			for (auto split : results) {
				UID parentGranuleID, granuleID;
				BlobGranuleSplitState splitState;
				Version version;
				if (split.expectedSize() == 0) {
					continue;
				}
				std::tie(parentGranuleID, granuleID) = decodeBlobGranuleSplitKey(split.key);
				std::tie(splitState, version) = decodeBlobGranuleSplitValue(split.value);
				const KeyRange range = blobGranuleSplitKeyRangeFor(parentGranuleID);
				if (splitState <= BlobGranuleSplitState::Initialized) {
					// the empty UID signifies that we need to find an owner (worker) for this range
					workerAssignments.insert(range, UID());
				}
			}

			if (!results.more) {
				break;
			}

			beginKey = results.readThrough.get();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// Step 3. Send assign requests for all the granules and transfer assignments
	// from local workerAssignments to bmData
	for (auto& range : workerAssignments.intersectingRanges(normalKeys)) {
		if (!range.value().present()) {
			continue;
		}

		bmData->workerAssignments.insert(range.range(), range.value().get());

		RangeAssignment raAssign;
		raAssign.isAssign = true;
		raAssign.worker = range.value().get();
		raAssign.keyRange = range.range();
		raAssign.assign = RangeAssignmentData(AssignRequestType::Reassign);
		bmData->rangesToAssign.send(raAssign);
	}

	return Void();
}

ACTOR Future<Void> chaosRangeMover(BlobManagerData* bmData) {
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

					// FIXME: with low probability, could immediately revoke it from the new assignment and move
					// it back right after to test that race

					state KeyRange range = randomRange.range();
					RangeAssignment revokeOld;
					revokeOld.isAssign = false;
					revokeOld.keyRange = range;
					revokeOld.revoke = RangeRevokeData(false);
					bmData->rangesToAssign.send(revokeOld);

					RangeAssignment assignNew;
					assignNew.isAssign = true;
					assignNew.keyRange = range;
					assignNew.assign = RangeAssignmentData(); // not a continue
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
	self->recruitingStream.set(self->recruitingStream.get() + 1);

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

			if (!self->deadWorkers.count(bwi.id())) {
				if (!self->workerAddresses.count(bwi.stableAddress()) && bwi.locality.dcId() == self->dcId) {
					self->workerAddresses.insert(bwi.stableAddress());
					self->workersById[bwi.id()] = bwi;
					self->workerStats[bwi.id()] = BlobWorkerStats();
					self->addActor.send(monitorBlobWorker(self, bwi));
				} else if (!self->workersById.count(bwi.id())) {
					self->addActor.send(killBlobWorker(self, bwi, false));
				}
			}

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
	self->recruitingStream.set(self->recruitingStream.get() - 1);
	self->restartRecruiting.trigger();
	return Void();
}

// Recruits blob workers in a loop
ACTOR Future<Void> blobWorkerRecruiter(
    BlobManagerData* self,
    Reference<IAsyncListener<RequestStream<RecruitBlobWorkerRequest>>> recruitBlobWorker) {
	state Future<RecruitBlobWorkerReply> fCandidateWorker;
	state RecruitBlobWorkerRequest lastRequest;

	// wait until existing blob workers have been acknowledged so we don't break recruitment invariants
	loop choose {
		when(wait(self->startRecruiting.onTrigger())) { break; }
	}

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

			if (!fCandidateWorker.isValid() || fCandidateWorker.isReady() ||
			    recruitReq.excludeAddresses != lastRequest.excludeAddresses) {
				lastRequest = recruitReq;
				// send req to cluster controller to get back a candidate worker we can recruit on
				fCandidateWorker =
				    brokenPromiseToNever(recruitBlobWorker->get().getReply(recruitReq, TaskPriority::BlobManager));
			}

			choose {
				// when we get back a worker we can use, we will try to initialize a blob worker onto that
				// process
				when(RecruitBlobWorkerReply candidateWorker = wait(fCandidateWorker)) {
					self->addActor.send(initializeBlobWorker(self, candidateWorker));
				}

				// when the CC changes, so does the request stream so we need to restart recruiting here
				when(wait(recruitBlobWorker->onChange())) { fCandidateWorker = Future<RecruitBlobWorkerReply>(); }

				// signal used to restart the loop and try to recruit the next blob worker
				when(wait(self->restartRecruiting.onTrigger())) {}
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

ACTOR Future<Void> haltBlobGranules(BlobManagerData* bmData) {
	std::vector<BlobWorkerInterface> blobWorkers = wait(getBlobWorkers(bmData->db));
	std::vector<Future<Void>> deregisterBlobWorkers;
	for (auto& worker : blobWorkers) {
		// TODO: send a special req to blob workers so they clean up granules/CFs
		bmData->addActor.send(
		    brokenPromiseToNever(worker.haltBlobWorker.getReply(HaltBlobWorkerRequest(bmData->epoch, bmData->id))));
		deregisterBlobWorkers.emplace_back(deregisterBlobWorker(bmData, worker));
	}
	waitForAll(deregisterBlobWorkers);

	return Void();
}

// TODO: refactor this into a common file
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

ACTOR Future<GranuleFiles> loadHistoryFiles(BlobManagerData* bmData, UID granuleID) {
	state Transaction tr(bmData->db);
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

/*
 * Deletes all files pertaining to the granule with id granuleId and
 * also removes the history entry for this granule from the system keyspace
 */
ACTOR Future<Void> fullyDeleteGranule(BlobManagerData* self, UID granuleId, KeyRef historyKey) {
	state Transaction tr(self->db);
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

	KeyRange filesRange = blobGranuleFileKeyRangeFor(granuleId);

	// get files
	GranuleFiles files = wait(loadHistoryFiles(self, granuleId));

	std::vector<Future<Void>> deletions;

	for (auto snapshotFile : files.snapshotFiles) {
		std::string fname = snapshotFile.filename;
		deletions.emplace_back(self->bstore->deleteFile(fname));
	}

	for (auto deltaFile : files.deltaFiles) {
		std::string fname = deltaFile.filename;
		deletions.emplace_back(self->bstore->deleteFile(fname));
	}

	wait(waitForAll(deletions));

	// delete metadata in FDB (history entry and file keys)
	loop {
		try {
			KeyRange fileRangeKey = blobGranuleFileKeyRangeFor(granuleId);
			tr.clear(historyKey);
			tr.clear(fileRangeKey);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return Void();
}

/*
 * For the granule with id granuleId, finds the first snapshot file at a
 * version <= pruneVersion and deletes all files older than it.
 */
ACTOR Future<Void> partiallyDeleteGranule(BlobManagerData* self, UID granuleId, Version pruneVersion) {
	state Transaction tr(self->db);
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

	KeyRange filesRange = blobGranuleFileKeyRangeFor(granuleId);

	GranuleFiles files = wait(loadHistoryFiles(self, granuleId));

	Version latestSnaphotVersion = invalidVersion;

	state std::vector<Future<Void>> deletions;

	for (int idx = files.snapshotFiles.size() - 1; idx >= 0; --idx) {
		// if we already found the latestSnapshotVersion, this snapshot can be deleted
		if (latestSnaphotVersion != invalidVersion) {
			std::string fname = files.snapshotFiles[idx].filename;
			deletions.emplace_back(self->bstore->deleteFile(fname));
		} else if (files.snapshotFiles[idx].version <= pruneVersion) {
			// otherwise if this is the FIRST snapshot file with version < pruneVersion,
			// then we found our latestSnapshotVersion (FIRST since we are traversing in reverse)
			latestSnaphotVersion = files.snapshotFiles[idx].version;
		}
	}

	ASSERT(latestSnaphotVersion != invalidVersion);

	// delete all delta files older than latestSnapshotVersion
	for (auto deltaFile : files.deltaFiles) {
		if (deltaFile.version < latestSnaphotVersion) {
			std::string fname = deltaFile.filename;
			deletions.emplace_back(self->bstore->deleteFile(fname));
		}
	}

	wait(waitForAll(deletions));
	return Void();
}

/*
 * This method is used to prune the range [startKey, endKey) at (and including) pruneVersion.
 * To do this, we do a BFS traversal starting at the active granules. Then we classify granules
 * in the history as nodes that can be fully deleted (i.e. their files and history can be deleted)
 * and nodes that can be partially deleted (i.e. some of their files can be deleted).
 * Once all this is done, we finally clear the pruneIntent key, if possible, to indicate we are done
 * processing this prune intent.
 *
 * TODO: communicate the prune to blob workers so they can clean up local memory
 */
ACTOR Future<Void> pruneRange(BlobManagerData* self, KeyRef startKey, KeyRef endKey, Version pruneVersion, bool force) {
	// queue of <range, startVersion, endVersion> for BFS traversal of history
	// TODO: consider using GranuleHistoryEntry, but that also makes it a little messy
	state std::queue<std::tuple<KeyRange, Version, Version>> historyEntryQueue;

	// stacks of <granuleId, historyKey> and <granuleId> to track which granules to delete
	state std::vector<std::tuple<UID, KeyRef>> toFullyDelete;
	state std::vector<UID> toPartiallyDelete;

	// set of granuleIds to track which granules we have already visited in traversal
	state std::unordered_set<UID> visited; // track which granules we have already visited in traversal

	KeyRange range(KeyRangeRef(startKey, endKey)); // range for [startKey, endKey)

	// find all active granules (that comprise the range) and add to the queue
	auto activeRanges = self->workerAssignments.intersectingRanges(range);
	state Transaction tr(self->db);
	tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

	for (auto& activeRange : activeRanges) {
		// only want to prune exact granules
		if (activeRange.begin() < startKey || activeRange.end() >= endKey) {
			continue;
		}
		loop {
			try {
				Optional<GranuleHistory> history = wait(getLatestGranuleHistory(&tr, activeRange.range()));
				ASSERT(history.present());
				historyEntryQueue.push({ activeRange.range(), history.get().version, MAX_VERSION });
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	while (!historyEntryQueue.empty()) {
		// process the node at the front of the queue and remove it
		KeyRange currRange;
		Version startVersion, endVersion;
		std::tie(currRange, startVersion, endVersion) = historyEntryQueue.front();
		historyEntryQueue.pop();

		// get the persisted history entry for this granule
		state Standalone<BlobGranuleHistoryValue> currHistoryNode;
		state KeyRef historyKey = blobGranuleHistoryKeyFor(currRange, startVersion);
		loop {
			try {
				Optional<Value> persistedHistory = wait(tr.get(historyKey));
				ASSERT(persistedHistory.present());
				currHistoryNode = decodeBlobGranuleHistoryValue(persistedHistory.get());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// if we already saw this node, skip it; otherwise, mark it as visited
		if (visited.count(currHistoryNode.granuleID)) {
			continue;
		}
		visited.insert(currHistoryNode.granuleID);

		// There are three cases this granule can fall into:
		// - if the granule's end version is at or before the prune version or this is a force delete,
		//   this granule should be completely deleted
		// - else if the startVersion <= pruneVersion, then G.startVersion <= pruneVersion < G.endVersion
		//   and so this granule should be partially deleted
		// - otherwise, this granule is active, so don't schedule it for deletion
		if (force || endVersion <= pruneVersion) {
			toFullyDelete.push_back({ currHistoryNode.granuleID, historyKey });
		} else if (startVersion <= pruneVersion) {
			toPartiallyDelete.push_back({ currHistoryNode.granuleID });
		}

		// add all of the node's parents to the queue
		for (auto& parent : currHistoryNode.parentGranules) {
			// the parent's end version is this node's startVersion
			historyEntryQueue.push({ parent.first, parent.second, startVersion });
		}
	}

	// The top of the stacks have the oldest ranges. This implies that for a granule located at
	// index i, it's parent must be located at some index j, where j > i. For this reason,
	// we delete granules in reverse order; this way, we will never end up with unreachable
	// nodes in the persisted history. Moreover, for any node that must be fully deleted,
	// any node that must be partially deleted must occur later on in the history. Thus,
	// we delete the 'toFullyDelete' granules first.
	//
	// Unfortunately we can't do multiple deletions in parallel because they might
	// race and we'll end up with unreachable nodes in the case of a crash

	for (int i = toFullyDelete.size() - 1; i >= 0; --i) {
		UID granuleId;
		KeyRef historyKey;
		std::tie(granuleId, historyKey) = toFullyDelete[i];
		wait(fullyDeleteGranule(self, granuleId, historyKey));
	}

	// TODO: could possibly do the partial deletes in parallel?
	for (int i = toPartiallyDelete.size() - 1; i >= 0; --i) {
		UID granuleId = toPartiallyDelete[i];
		wait(partiallyDeleteGranule(self, granuleId, pruneVersion));
	}

	// There could have been another pruneIntent that got written for this table while we
	// were processing this one. If that is the case, we should not clear the key. Otherwise,
	// we should clear the key to indicate the work is done.
	state Reference<ReadYourWritesTransaction> rywTr = makeReference<ReadYourWritesTransaction>(self->db);
	rywTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	rywTr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {
			state RangeResult pruneIntent =
			    wait(krmGetRanges(rywTr, blobGranulePruneKeys.begin, range, 1, GetRangeLimits::BYTE_LIMIT_UNLIMITED));
			ASSERT(pruneIntent.size() == 1);

			if (decodeBlobGranulePruneValue(pruneIntent[0].value).first == pruneVersion) {
				rywTr->clear(pruneIntent[0].key);
				wait(rywTr->commit());
			}
			break;
		} catch (Error& e) {
			wait(rywTr->onError(e));
		}
	}
}

/*
 * This monitor watches for changes to a key K that gets updated whenever there is a new prune intent.
 * On this change, we scan through all blobGranulePruneKeys (which look like <startKey, endKey>=<prune_version,
 * force>) and prune any intents.
 *
 * Once the prune has succeeded, we clear the key IF the version is still the same one that was pruned.
 * That way, if another prune intent arrived for the same range while we were working on an older one,
 * we wouldn't end up clearing the intent.
 *
 * When watching for changes, we might end up in scenarios where we failed to do the work
 * for a prune intent even though the watch was triggered (maybe the BM had a blip). This is problematic
 * if the intent is a force and there isn't another prune intent for quite some time. To remedy this,
 * if we don't see a watch change in X (configurable) seconds, we will just sweep through the prune intents,
 * consolidating any work we might have missed before.
 *
 * Note: we could potentially use a changefeed here to get the exact pruneIntent that was added
 * rather than iterating through all of them, but this might have too much overhead for latency
 * improvements we don't really need here (also we need to go over all prune intents anyways in the
 * case that the timer is up before any new prune intents arrive).
 */
ACTOR Future<Void> monitorPruneKeys(BlobManagerData* self) {
	try {
		loop {
			state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			// Wait for the watch to change, or some time to expire (whichever comes first)
			// before checking through the prune intents
			loop {
				try {
					state Future<Void> watchPruneIntentsChange = tr->watch(blobGranulePruneChangeKey);
					wait(tr->commit());
					wait(timeout(watchPruneIntentsChange, SERVER_KNOBS->BG_PRUNE_TIMEOUT, Void()));
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}

			// loop through all prune intentions and do prune work accordingly
			state KeyRef beginKey = normalKeys.begin;
			loop {
				try {
					// TODO: replace 10000 with a knob
					KeyRange nextRange(KeyRangeRef(beginKey, normalKeys.end));
					state RangeResult pruneIntents = wait(krmGetRanges(
					    tr, blobGranulePruneKeys.begin, nextRange, 10000, GetRangeLimits::BYTE_LIMIT_UNLIMITED));

					// TODO: would we miss a range [pruneIntents[9999], pruneIntents[10000]) because of the `more`?
					//       Or does `readThrough` take care of this? We also do this in recoverBlobManager
					for (int rangeIdx = 0; rangeIdx < pruneIntents.size() - 1; ++rangeIdx) {
						if (pruneIntents[rangeIdx].value.size() == 0) {
							continue;
						}
						KeyRef rangeStartKey = pruneIntents[rangeIdx].key;
						KeyRef rangeEndKey = pruneIntents[rangeIdx + 1].key;
						Version pruneVersion;
						bool force;
						std::tie(pruneVersion, force) = decodeBlobGranulePruneValue(pruneIntents[rangeIdx].value);

						// TODO: should we add this to an actor collection or a list of futures?
						// Probably because still need to handle the case of one prune at version V and then timer
						// expires and we start another prune again at version V. we need to keep track of what's in
						// progress. That brings another problem though: what happens if something is in progress and
						// fails... One way to prevent this is to not iterate over the prunes until the last iteration
						// is done (i.e waitForAll)
						//
						// ErrorOr would prob be what we need here

						pruneRange(self, rangeStartKey, rangeEndKey, pruneVersion, force);
					}

					if (!pruneIntents.more) {
						break;
					}

					beginKey = pruneIntents.readThrough.get();
				} catch (Error& e) {
					// TODO: other errors here from pruneRange?
					wait(tr->onError(e));
				}
			}
		}
	} catch (Error& e) {
		if (BM_DEBUG) {
			printf("monitorPruneKeys got error %s\n", e.name());
		}
		throw e;
	}
}

ACTOR Future<Void> blobManager(BlobManagerInterface bmInterf,
                               Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                               int64_t epoch) {
	state BlobManagerData self(deterministicRandom()->randomUniqueID(),
	                           openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True),
	                           bmInterf.locality.dcId());

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
		fmt::print("Blob manager acquired lock at epoch {}\n", epoch);
	}

	try {
		if (BM_DEBUG) {
			printf("BM constructing backup container from %s\n", SERVER_KNOBS->BG_URL.c_str());
		}
		self.bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL);
		if (BM_DEBUG) {
			printf("BM constructed backup container\n");
		}
	} catch (Error& e) {
		if (BM_DEBUG) {
			printf("BM got backup container init error %s\n", e.name());
		}
		throw e;
	}

	// although we start the recruiter, we wait until existing workers are ack'd
	auto recruitBlobWorker = IAsyncListener<RequestStream<RecruitBlobWorkerRequest>>::create(
	    dbInfo, [](auto const& info) { return info.clusterInterface.recruitBlobWorker; });
	self.addActor.send(blobWorkerRecruiter(&self, recruitBlobWorker));

	// we need to recover the old blob manager's state (e.g. granule assignments) before
	// before the new blob manager does anything
	wait(recoverBlobManager(&self));

	self.addActor.send(monitorClientRanges(&self));
	self.addActor.send(rangeAssigner(&self));

	if (BUGGIFY) {
		self.addActor.send(chaosRangeMover(&self));
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
			when(state HaltBlobGranulesRequest req = waitNext(bmInterf.haltBlobGranules.getFuture())) {
				wait(haltBlobGranules(&self));
				req.reply.send(Void());
				TraceEvent("BlobGranulesHalted", bmInterf.id()).detail("ReqID", req.requesterID);
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
TEST_CASE(":/blobmanager/updateranges") {
	KeyRangeMap<bool> knownBlobRanges(false, normalKeys.end);
	Arena ar;

	VectorRef<KeyRangeRef> added;
	VectorRef<KeyRangeRef> removed;

	StringRef active = LiteralStringRef("1");
	StringRef inactive = StringRef();

	RangeResult dbDataEmpty;
	std::vector<std::pair<KeyRangeRef, bool>> kbrRanges;

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
