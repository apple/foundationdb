/*
 * BlobManager.actor.cpp
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

#include <algorithm>
#include <limits>
#include <sstream>
#include <queue>
#include <vector>
#include <unordered_map>

#include "fdbclient/ServerKnobs.h"
#include "fdbrpc/simulator.h"
#include "fmt/format.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/BlobManagerInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/BlobGranuleValidation.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

/*
 * The Blob Manager is responsible for managing range granules, and recruiting and monitoring Blob Workers.
 */

#define BM_DEBUG false
#define BM_PURGE_DEBUG false

void handleClientBlobRange(KeyRangeMap<bool>* knownBlobRanges,
                           Arena& ar,
                           VectorRef<KeyRangeRef>* rangesToAdd,
                           VectorRef<KeyRangeRef>* rangesToRemove,
                           KeyRef rangeStart,
                           KeyRef rangeEnd,
                           bool rangeActive) {
	if (BM_DEBUG) {
		fmt::print(
		    "db range [{0} - {1}): {2}\n", rangeStart.printable(), rangeEnd.printable(), rangeActive ? "T" : "F");
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
					fmt::print("BM Adding client range [{0} - {1})\n",
					           overlapStart.printable().c_str(),
					           overlapEnd.printable().c_str());
				}
				rangesToAdd->push_back_deep(ar, overlap);
			} else {
				if (BM_DEBUG) {
					fmt::print("BM Removing client range [{0} - {1})\n",
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
		fmt::print("Updating {0} client blob ranges", dbBlobRanges.size() / 2);
		for (int i = 0; i < dbBlobRanges.size() - 1; i += 2) {
			fmt::print("  [{0} - {1})", dbBlobRanges[i].key.printable(), dbBlobRanges[i + 1].key.printable());
		}
		printf("\n");
	}
	// essentially do merge diff of current known blob ranges and new ranges, to assign new ranges to
	// workers and revoke old ranges from workers

	// basically, for any range that is set in results that isn't set in ranges, assign the range to the
	// worker. for any range that isn't set in results that is set in ranges, revoke the range from the
	// worker. and, update ranges to match results as you go

	// SOMEDAY: could change this to O(N) instead of O(NLogN) by doing a sorted merge instead of requesting the
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
					fmt::print("Found invalid blob range start {0}\n", dbBlobRanges[i].key.printable());
				}
				break;
			}
			bool active = dbBlobRanges[i].value == blobRangeActive;
			if (active) {
				if (BM_DEBUG) {
					fmt::print("BM sees client range [{0} - {1})\n",
					           dbBlobRanges[i].key.printable(),
					           dbBlobRanges[i + 1].key.printable());
				}
			}
			KeyRef endKey = dbBlobRanges[i + 1].key;
			if (endKey > normalKeys.end) {
				if (BM_DEBUG) {
					fmt::print("Removing system keyspace from blob range [{0} - {1})\n",
					           dbBlobRanges[i].key.printable(),
					           endKey.printable());
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
			fmt::print("  [{0} - {1}): {2}\n", r.begin().printable(), r.end().printable(), r.value() ? "T" : "F");
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
	Optional<std::pair<UID, Error>> previousFailure;

	// I tried doing this with a union and it was just kind of messy
	Optional<RangeAssignmentData> assign;
	Optional<RangeRevokeData> revoke;
};

// SOMEDAY: track worker's reads/writes eventually
// FIXME: namespace?
struct BlobWorkerInfo {
	int numGranulesAssigned;

	BlobWorkerInfo(int numGranulesAssigned = 0) : numGranulesAssigned(numGranulesAssigned) {}
};

enum BoundaryEvalType { UNKNOWN, MERGE, SPLIT };

struct BoundaryEvaluation {
	int64_t epoch;
	int64_t seqno;
	BoundaryEvalType type;
	Future<Void> inProgress;
	int64_t originalEpoch;
	int64_t originalSeqno;

	BoundaryEvaluation() : epoch(0), seqno(0), type(UNKNOWN), originalEpoch(0), originalSeqno(0) {}
	BoundaryEvaluation(int64_t epoch,
	                   int64_t seqno,
	                   BoundaryEvalType type,
	                   int64_t originalEpoch,
	                   int64_t originalSeqno)
	  : epoch(epoch), seqno(seqno), type(type), originalEpoch(originalEpoch), originalSeqno(originalSeqno) {
		ASSERT(type != UNKNOWN);
	}

	bool operator==(const BoundaryEvaluation& other) const {
		return epoch == other.epoch && seqno == other.seqno && type == other.type;
	}

	bool operator<(const BoundaryEvaluation& other) {
		// if (epoch, seqno) don't match, go by (epoch, seqno)
		if (epoch == other.epoch && seqno == other.seqno) {
			return type < other.type;
		}
		return epoch < other.epoch || (epoch == other.epoch && seqno < other.seqno);
	}

	bool isOlderThanOriginal(const BoundaryEvaluation& other) {
		return originalEpoch < other.originalEpoch ||
		       (originalEpoch == other.originalEpoch && originalSeqno < other.originalSeqno);
	}

	std::string toString() const {
		return fmt::format("{0} @ ({1}, {2})",
		                   type == BoundaryEvalType::UNKNOWN ? "unknown"
		                                                     : (type == BoundaryEvalType::MERGE ? "merge" : "split"),
		                   epoch,
		                   seqno);
	}
};

struct BlobManagerStats {
	CounterCollection cc;

	Counter granuleSplits;
	Counter granuleWriteHotSplits;
	Counter granuleMerges;
	Counter ccGranulesChecked;
	Counter ccRowsChecked;
	Counter ccBytesChecked;
	Counter ccMismatches;
	Counter ccTimeouts;
	Counter ccErrors;
	Counter purgesProcessed;
	Counter granulesFullyPurged;
	Counter granulesPartiallyPurged;
	Counter filesPurged;
	Future<Void> logger;
	int64_t activeMerges;
	int64_t blockedAssignments;

	// Current stats maintained for a given blob worker process
	explicit BlobManagerStats(UID id,
	                          double interval,
	                          int64_t epoch,
	                          std::unordered_map<UID, BlobWorkerInterface>* workers,
	                          std::unordered_map<Key, bool>* mergeHardBoundaries,
	                          std::unordered_map<Key, BlobGranuleMergeBoundary>* mergeBoundaries)
	  : cc("BlobManagerStats", id.toString()), granuleSplits("GranuleSplits", cc),
	    granuleWriteHotSplits("GranuleWriteHotSplits", cc), granuleMerges("GranuleMerges", cc),
	    ccGranulesChecked("CCGranulesChecked", cc), ccRowsChecked("CCRowsChecked", cc),
	    ccBytesChecked("CCBytesChecked", cc), ccMismatches("CCMismatches", cc), ccTimeouts("CCTimeouts", cc),
	    ccErrors("CCErrors", cc), purgesProcessed("PurgesProcessed", cc),
	    granulesFullyPurged("GranulesFullyPurged", cc), granulesPartiallyPurged("GranulesPartiallyPurged", cc),
	    filesPurged("FilesPurged", cc), activeMerges(0), blockedAssignments(0) {
		specialCounter(cc, "WorkerCount", [workers]() { return workers->size(); });
		specialCounter(cc, "Epoch", [epoch]() { return epoch; });
		specialCounter(cc, "ActiveMerges", [this]() { return this->activeMerges; });
		specialCounter(cc, "HardBoundaries", [mergeHardBoundaries]() { return mergeHardBoundaries->size(); });
		specialCounter(cc, "SoftBoundaries", [mergeBoundaries]() { return mergeBoundaries->size(); });
		specialCounter(cc, "BlockedAssignments", [this]() { return this->blockedAssignments; });
		logger = cc.traceCounters("BlobManagerMetrics", id, interval, "BlobManagerMetrics");
	}
};

enum MergeCandidateState {
	MergeCandidateUnknown,
	MergeCandidateCannotMerge,
	MergeCandidateCanMerge,
	MergeCandidateMerging
};

// The current merge algorithm, skipping just granules that will be merge-eligible on the next pass, but not
// their neighbors, is optimal for guaranteeing merges to make progress where possible, with decently
// optimal but not globally optimal merge behavior.
// Alternative algorithms include not doing a two-pass consideration at all and immediately considering
// all merge candidates, which guarantees the most progress but pretty much guarantees undesirably
// suboptimal merge decisions, because of the time variance of granules becoming merge candidates. Or,
// also skipping adjacent eligible granules in addition to the one that will be eligible next pass,
// which ensures optimally large merges in a future pass, but adds decent delay to doing the merge. Or,
// smarter considering of merge candidates adjacent to the one that will be eligible next pass
// (depending on whether potential future merges with adjacent ones could include this candidate), which
// would be the best of both worlds, but would add a decent amount of code complexity.
struct MergeCandidateInfo {
	MergeCandidateState st;
	UID granuleID;
	Version startVersion;
	// This is if this candidate has been seen by the merge checker before.
	bool seen;

	MergeCandidateInfo() : st(MergeCandidateUnknown), startVersion(invalidVersion), seen(false) {}

	MergeCandidateInfo(MergeCandidateState st) : st(st), startVersion(invalidVersion), seen(false) {
		ASSERT(st != MergeCandidateCanMerge);
	}
	MergeCandidateInfo(UID granuleID, Version startVersion)
	  : st(MergeCandidateCanMerge), granuleID(granuleID), startVersion(startVersion), seen(false) {}

	bool canMerge() const { return st == MergeCandidateCanMerge; }

	bool mergeEligible() const { return st == MergeCandidateCanMerge && seen; }
};

struct BlobGranuleSplitPoints {
	Standalone<VectorRef<KeyRef>> keys;
	std::unordered_map<Key, BlobGranuleMergeBoundary> boundaries;
};

struct BlobManagerData : NonCopyable, ReferenceCounted<BlobManagerData> {
	UID id;
	Database db;
	Optional<Key> dcId;
	PromiseStream<Future<Void>> addActor;
	Promise<Void> doLockCheck;

	BlobManagerStats stats;

	Reference<BlobConnectionProvider> bstore;

	std::unordered_map<UID, BlobWorkerInterface> workersById;
	std::unordered_map<UID, BlobWorkerInfo> workerStats; // mapping between workerID -> workerStats
	std::unordered_set<NetworkAddress> workerAddresses;
	std::unordered_set<UID> deadWorkers;
	KeyRangeMap<UID> workerAssignments;
	KeyRangeActorMap assignsInProgress;
	KeyRangeMap<BoundaryEvaluation> boundaryEvaluations;
	KeyRangeMap<bool> knownBlobRanges;
	BGTenantMap tenantData;
	KeyRangeMap<MergeCandidateInfo> mergeCandidates; // granule range to granule id + start version.
	KeyRangeMap<Version> activeGranuleMerges; // range map of active granule merges, because range in boundaryEval
	                                          // doesn't correspond to merge range. invalidVersion is no merge,
	                                          // 0 is no merge version determined yet
	// TODO: consider switching to an iterator approach.
	std::unordered_map<Key, bool> mergeHardBoundaries;
	std::unordered_map<Key, BlobGranuleMergeBoundary> mergeBoundaries;
	CoalescedKeyRangeMap<bool> forcePurgingRanges;

	FlowLock concurrentMergeChecks;

	AsyncTrigger startRecruiting;
	Debouncer restartRecruiting;
	std::set<NetworkAddress> recruitingLocalities; // the addrs of the workers being recruited on
	AsyncVar<int> recruitingStream;
	Promise<Void> foundBlobWorkers;
	Promise<Void> doneRecovering;
	Promise<Void> loadedClientRanges;

	int64_t epoch;
	int64_t seqNo = 1;
	int64_t manifestDumperSeqNo = 1;

	Promise<Void> iAmReplaced;

	BlobManagerData(UID id,
	                Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	                Database db,
	                Optional<Key> dcId,
	                int64_t epoch)
	  : id(id), db(db), dcId(dcId),
	    stats(id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, epoch, &workersById, &mergeHardBoundaries, &mergeBoundaries),
	    knownBlobRanges(false, normalKeys.end), tenantData(BGTenantMap(dbInfo)),
	    mergeCandidates(MergeCandidateInfo(MergeCandidateUnknown), normalKeys.end),
	    activeGranuleMerges(invalidVersion, normalKeys.end), forcePurgingRanges(false, normalKeys.end),
	    concurrentMergeChecks(SERVER_KNOBS->BLOB_MANAGER_CONCURRENT_MERGE_CHECKS),
	    restartRecruiting(SERVER_KNOBS->DEBOUNCE_RECRUITING_DELAY), recruitingStream(0), epoch(epoch) {}

	// only initialize blob store if actually needed
	void initBStore() {
		if (!bstore.isValid() && SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			if (BM_DEBUG) {
				fmt::print("BM {} constructing backup container from {}\n", epoch, SERVER_KNOBS->BG_URL.c_str());
			}
			bstore = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
			if (BM_DEBUG) {
				fmt::print("BM {} constructed backup container\n", epoch);
			}
		}
	}

	bool isMergeActive(const KeyRangeRef& range) {
		auto ranges = activeGranuleMerges.intersectingRanges(range);
		for (auto& it : ranges) {
			if (it.value() != invalidVersion) {
				return true;
			}
		}
		return false;
	}

	Version activeMergeVersion(const KeyRangeRef& range) {
		auto ranges = activeGranuleMerges.intersectingRanges(range);
		Version v = invalidVersion;
		for (auto& it : ranges) {
			v = std::max(v, it.cvalue());
		}
		return v;
	}

	// FIXME: is it possible for merge/split/re-merge to call this with same range but a different granule id or
	// startVersion? Unlikely but could cause weird history problems
	void setMergeCandidate(const KeyRangeRef& range, UID granuleID, Version startVersion) {
		// if this granule is not an active granule, it can't be merged
		auto gIt = workerAssignments.rangeContaining(range.begin);
		if (gIt->begin() != range.begin || gIt->end() != range.end) {
			CODE_PROBE(true, "non-active granule reported merge eligible, ignoring");
			if (BM_DEBUG) {
				fmt::print(
				    "BM {0} Ignoring Merge Candidate [{1} - {2}): range mismatch with active granule [{3} - {4})\n",
				    epoch,
				    range.begin.printable(),
				    range.end.printable(),
				    gIt->begin().printable(),
				    gIt->end().printable());
			}
			return;
		}
		// Want this to be idempotent. If a granule was already reported as merge-eligible, we want to use the existing
		// merge and mergeNow state.
		auto it = mergeCandidates.rangeContaining(range.begin);

		if (it->begin() == range.begin && it.end() == range.end) {
			if (it->cvalue().st != MergeCandidateCanMerge) {
				// same range, just update
				it->value() = MergeCandidateInfo(granuleID, startVersion);
			} else {
				// else no-op, but validate data
				ASSERT(granuleID == it->cvalue().granuleID);
				ASSERT(startVersion == it->cvalue().startVersion);
			}
		} else if (it->cvalue().st != MergeCandidateMerging) {
			mergeCandidates.insert(range, MergeCandidateInfo(granuleID, startVersion));
		}
	}

	void setMergeCandidate(const KeyRangeRef& range, MergeCandidateState st) {
		ASSERT(st != MergeCandidateCanMerge);
		mergeCandidates.insert(range, MergeCandidateInfo(st));
	}

	void clearMergeCandidate(const KeyRangeRef& range) { setMergeCandidate(range, MergeCandidateCannotMerge); }

	bool isForcePurging(const KeyRangeRef& range) {
		auto ranges = forcePurgingRanges.intersectingRanges(range);
		for (auto& it : ranges) {
			if (it.value()) {
				return true;
			}
		}
		return false;
	}

	bool maybeInjectTargetedRestart() {
		// inject a BW restart at most once per test
		if (g_network->isSimulated() && !g_simulator->speedUpSimulation &&
		    now() > g_simulator->injectTargetedBMRestartTime) {
			CODE_PROBE(true, "Injecting BM targeted restart");
			TraceEvent("SimBMInjectTargetedRestart", id);
			g_simulator->injectTargetedBMRestartTime = std::numeric_limits<double>::max();
			iAmReplaced.send(Void());
			return true;
		}
		return false;
	}
};

// Helper function for alignKeys().
// This attempts to do truncation and compares with the last key in splitPoints.keys.
static void alignKeyBoundary(Reference<BlobManagerData> bmData,
                             Reference<GranuleTenantData> tenantData,
                             KeyRef key,
                             int offset,
                             BlobGranuleSplitPoints& splitPoints) {
	Standalone<VectorRef<KeyRef>>& keys = splitPoints.keys;
	std::unordered_map<Key, BlobGranuleMergeBoundary>& boundaries = splitPoints.boundaries;
	KeyRef alignedKey = key;
	Tuple t, t2;

	if (!offset) {
		keys.push_back_deep(keys.arena(), alignedKey);
		return;
	}

	// If this is tenant aware code.
	if (tenantData.isValid()) {
		alignedKey = alignedKey.removePrefix(tenantData->entry.prefix);
	}
	try {
		t = Tuple::unpackUserType(alignedKey, true);
		if (t.size() > offset) {
			t2 = t.subTuple(0, t.size() - offset);
			alignedKey = t2.pack();
		}
	} catch (Error& e) {
		if (e.code() != error_code_invalid_tuple_data_type) {
			throw;
		}
	}
	if (tenantData.isValid()) {
		alignedKey = alignedKey.withPrefix(tenantData->entry.prefix, keys.arena());
	}

	// Only add the alignedKey if it's larger than the last key. If it's the same, drop the split.
	if (alignedKey <= keys.back()) {
		// Set split boundary.
		BlobGranuleMergeBoundary boundary = { /*buddy=*/true };
		boundaries[key] = boundary;
		keys.push_back_deep(keys.arena(), key);
	} else {
		keys.push_back_deep(keys.arena(), alignedKey);
	}
}

ACTOR Future<BlobGranuleSplitPoints> alignKeys(Reference<BlobManagerData> bmData,
                                               KeyRange granuleRange,
                                               Standalone<VectorRef<KeyRef>> splits) {
	state BlobGranuleSplitPoints splitPoints;

	state int offset = SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET;
	if (offset <= 0) {
		splitPoints.keys = splits;
		return splitPoints;
	}

	splitPoints.keys.push_back_deep(splitPoints.keys.arena(), splits.front());

	state Transaction tr = Transaction(bmData->db);
	state int idx = 1;
	state Reference<GranuleTenantData> tenantData;
	wait(store(tenantData, bmData->tenantData.getDataForGranule(granuleRange)));
	while (SERVER_KNOBS->BG_METADATA_SOURCE == "tenant" && !tenantData.isValid()) {
		// this is a bit of a hack, but if we know this range is supposed to have a tenant, and it doesn't, just wait
		wait(delay(1.0));
		wait(store(tenantData, bmData->tenantData.getDataForGranule(granuleRange)));
	}
	for (; idx < splits.size() - 1; idx++) {
		loop {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				// Get the next full key in the granule.
				RangeResult nextKeyRes = wait(
				    tr.getRange(firstGreaterOrEqual(splits[idx]), lastLessThan(splits[idx + 1]), GetRangeLimits(1)));
				if (nextKeyRes.size() == 0) {
					break;
				}

				alignKeyBoundary(bmData, tenantData, nextKeyRes[0].key, offset, splitPoints);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	splitPoints.keys.push_back_deep(splitPoints.keys.arena(), splits.back());

	return splitPoints;
}

ACTOR Future<BlobGranuleSplitPoints> splitRange(Reference<BlobManagerData> bmData,
                                                KeyRange range,
                                                bool writeHot,
                                                bool initialSplit) {
	state BlobGranuleSplitPoints splitPoints;
	try {
		if (BM_DEBUG) {
			fmt::print("Splitting new range [{0} - {1}): {2}\n",
			           range.begin.printable(),
			           range.end.printable(),
			           writeHot ? "hot" : "normal");
		}
		state StorageMetrics estimated = wait(bmData->db->getStorageMetrics(range, CLIENT_KNOBS->TOO_MANY));

		if (BM_DEBUG) {
			fmt::print("Estimated bytes for [{0} - {1}): {2}\n",
			           range.begin.printable(),
			           range.end.printable(),
			           estimated.bytes);
		}

		int64_t splitThreshold = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
		if (!initialSplit) {
			// If we have X MB target granule size, we want to do the initial split to split up into X MB chunks.
			// However, if we already have a granule that we are evaluating for split, if we split it as soon as it is
			// larger than X MB, we will end up with 2 X/2 MB granules.
			// To ensure an average size of X MB, we split granules at 4/3*X, so that they range between 2/3*X and
			// 4/3*X, averaging X
			splitThreshold = (splitThreshold * 4) / 3;
		}
		// if write-hot, we want to be able to split smaller, but not infinitely. Allow write-hot granules to be 3x
		// smaller
		// TODO knob?
		// TODO: re-evaluate after we have granule merging?
		if (writeHot) {
			splitThreshold /= 3;
		}
		CODE_PROBE(writeHot, "Change feed write hot split");
		if (estimated.bytes > splitThreshold) {
			// only split on bytes and write rate
			state StorageMetrics splitMetrics;
			splitMetrics.bytes = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
			splitMetrics.bytesPerKSecond = SERVER_KNOBS->SHARD_SPLIT_BYTES_PER_KSEC;
			if (writeHot) {
				splitMetrics.bytesPerKSecond = std::min(splitMetrics.bytesPerKSecond, estimated.bytesPerKSecond / 2);
				splitMetrics.bytesPerKSecond =
				    std::max(splitMetrics.bytesPerKSecond, SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC);
			}
			splitMetrics.iosPerKSecond = splitMetrics.infinity;
			splitMetrics.bytesReadPerKSecond = splitMetrics.infinity;

			state PromiseStream<Key> resultStream;
			state Standalone<VectorRef<KeyRef>> keys;
			// SplitMetrics.bytes / 3 as min split size because of same splitThreshold logic above.
			state Future<Void> streamFuture = bmData->db->splitStorageMetricsStream(
			    resultStream, range, splitMetrics, estimated, splitMetrics.bytes / 3);
			loop {
				try {
					Key k = waitNext(resultStream.getFuture());
					keys.push_back_deep(keys.arena(), k);
				} catch (Error& e) {
					if (e.code() != error_code_end_of_stream) {
						throw;
					}
					break;
				}
			}

			// We only need to align the keys if there is a proposed split.
			if (keys.size() > 2) {
				BlobGranuleSplitPoints _splitPoints = wait(alignKeys(bmData, range, keys));
				splitPoints = _splitPoints;
			} else {
				splitPoints.keys = keys;
			}

			ASSERT(splitPoints.keys.size() >= 2);
			ASSERT(splitPoints.keys.front() == range.begin);
			ASSERT(splitPoints.keys.back() == range.end);
			return splitPoints;
		} else {
			CODE_PROBE(writeHot, "Not splitting write-hot because granules would be too small");
			if (BM_DEBUG) {
				printf("Not splitting range\n");
			}
			splitPoints.keys.push_back_deep(splitPoints.keys.arena(), range.begin);
			splitPoints.keys.push_back_deep(splitPoints.keys.arena(), range.end);
			return splitPoints;
		}
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}
		// SplitStorageMetrics explicitly has a SevError if it gets an error, so no errors should propagate here
		TraceEvent(SevError, "BlobManagerUnexpectedErrorSplitRange", bmData->id)
		    .error(e)
		    .detail("Epoch", bmData->epoch);
		ASSERT_WE_THINK(false);

		// if not simulation, kill the BM
		if (bmData->iAmReplaced.canBeSet()) {
			bmData->iAmReplaced.sendError(e);
		}
		throw e;
	}
}

// Picks a worker with the fewest number of already assigned ranges.
// If there is a tie, picks one such worker at random.
ACTOR Future<UID> pickWorkerForAssign(Reference<BlobManagerData> bmData,
                                      Optional<std::pair<UID, Error>> previousFailure) {
	// wait until there are BWs to pick from
	loop {
		state bool wasZeroWorkers = false;
		while (bmData->workerStats.size() == 0) {
			wasZeroWorkers = true;
			CODE_PROBE(true, "BM wants to assign range, but no workers available");
			if (BM_DEBUG) {
				fmt::print("BM {0} waiting for blob workers before assigning granules\n", bmData->epoch);
			}
			bmData->restartRecruiting.trigger();
			wait(bmData->recruitingStream.onChange() || bmData->foundBlobWorkers.getFuture());
		}
		if (wasZeroWorkers) {
			// Add a bit of delay. If we were at zero workers, don't immediately assign all granules to the first worker
			// we recruit
			wait(delay(0.1));
		}
		if (bmData->workerStats.size() != 0) {
			break;
		}
		// if in the post-zero workers delay, we went back down to zero workers, re-loop
	}

	int minGranulesAssigned = INT_MAX;
	std::vector<UID> eligibleWorkers;

	// because lowest number of granules worker(s) might not exactly have the lowest memory for various reasons, if we
	// got blob_worker_full as the error last time, sometimes just pick a random worker that wasn't the last one we
	// tried
	if (bmData->workerStats.size() >= 2 && previousFailure.present() &&
	    previousFailure.get().second.code() == error_code_blob_worker_full && deterministicRandom()->coinflip()) {
		CODE_PROBE(true, "randomly picking worker due to blob_worker_full");
		eligibleWorkers.reserve(bmData->workerStats.size());
		for (auto& it : bmData->workerStats) {
			if (it.first != previousFailure.get().first) {
				eligibleWorkers.push_back(it.first);
			}
		}
		ASSERT(!eligibleWorkers.empty());
		int randomIdx = deterministicRandom()->randomInt(0, eligibleWorkers.size());
		if (BM_DEBUG) {
			fmt::print("picked worker {0} randomly since previous attempt got blob_worker_full\n",
			           eligibleWorkers[randomIdx].toString().substr(0, 5));
		}

		return eligibleWorkers[randomIdx];
	}

	for (auto const& worker : bmData->workerStats) {
		UID currId = worker.first;
		int granulesAssigned = worker.second.numGranulesAssigned;

		// if previous attempt failed and that worker is still present, ignore it
		if (bmData->workerStats.size() >= 2 && previousFailure.present() && previousFailure.get().first == currId) {
			continue;
		}

		if (granulesAssigned <= minGranulesAssigned) {
			if (granulesAssigned < minGranulesAssigned) {
				eligibleWorkers.clear();
				minGranulesAssigned = granulesAssigned;
			}
			eligibleWorkers.emplace_back(currId);
		}
	}

	// pick a random worker out of the eligible workers
	ASSERT(eligibleWorkers.size() > 0);
	int idx = deterministicRandom()->randomInt(0, eligibleWorkers.size());
	if (BM_DEBUG) {
		fmt::print("picked worker {0}, which has a minimal number ({1}) of granules assigned\n",
		           eligibleWorkers[idx].toString().substr(0, 5),
		           minGranulesAssigned);
	}

	return eligibleWorkers[idx];
}

// circular dependency between handleRangeAssign and doRangeAssignment
static bool handleRangeAssign(Reference<BlobManagerData> bmData, RangeAssignment assignment);

ACTOR Future<Void> doRangeAssignment(Reference<BlobManagerData> bmData,
                                     RangeAssignment assignment,
                                     Optional<UID> workerID,
                                     int64_t epoch,
                                     int64_t seqNo) {
	state bool blockedWaitingForWorker = false;
	// WorkerId is set, except in case of assigning to any worker. Then we pick the worker to assign to in here
	try {
		// inject delay into range assignments
		if (BUGGIFY_WITH_PROB(0.05)) {
			wait(delay(deterministicRandom()->random01()));
		} else {
			// otherwise, do delay(0) to ensure rest of code in calling handleRangeAssign runs, before this function can
			// recursively call handleRangeAssign on error
			wait(delay(0.0));
		}
		if (!workerID.present()) {
			ASSERT(assignment.isAssign && assignment.assign.get().type != AssignRequestType::Continue);

			blockedWaitingForWorker = true;
			if (!assignment.previousFailure.present()) {
				// if not already blocked, now blocked
				++bmData->stats.blockedAssignments;
			}

			UID _workerId = wait(pickWorkerForAssign(bmData, assignment.previousFailure));
			if (BM_DEBUG) {
				fmt::print("Chose BW {0} for seqno {1} in BM {2}\n", _workerId.toString(), seqNo, bmData->epoch);
			}
			workerID = _workerId;
			// We don't have to check for races with an overlapping assignment because it would insert over us in the
			// actor map, cancelling this actor before it got here
			bmData->workerAssignments.insert(assignment.keyRange, workerID.get());

			if (bmData->workerStats.count(workerID.get())) {
				bmData->workerStats[workerID.get()].numGranulesAssigned += 1;
			}

			if (!assignment.previousFailure.present()) {
				// if only blocked waiting for worker, now not blocked
				--bmData->stats.blockedAssignments;
			}
		}
	} catch (Error& e) {
		if (assignment.previousFailure.present() || blockedWaitingForWorker) {
			--bmData->stats.blockedAssignments;
		}
		throw e;
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} {1} range [{2} - {3}) @ ({4}, {5}) to {6}\n",
		           bmData->epoch,
		           assignment.isAssign ? "assigning" : "revoking",
		           assignment.keyRange.begin.printable(),
		           assignment.keyRange.end.printable(),
		           epoch,
		           seqNo,
		           workerID.get().toString());
	}

	try {
		if (assignment.isAssign) {
			ASSERT(assignment.assign.present());
			ASSERT(!assignment.revoke.present());

			AssignBlobRangeRequest req;
			req.keyRange = KeyRangeRef(StringRef(req.arena, assignment.keyRange.begin),
			                           StringRef(req.arena, assignment.keyRange.end));
			req.managerEpoch = epoch;
			req.managerSeqno = seqNo;
			req.type = assignment.assign.get().type;

			// if that worker isn't alive anymore, add the range back into the stream
			if (bmData->workersById.count(workerID.get()) == 0) {
				throw no_more_servers();
			}
			state Future<Void> assignFuture = bmData->workersById[workerID.get()].assignBlobRangeRequest.getReply(req);

			if (BUGGIFY) {
				// wait for request to actually send
				wait(delay(0));
				if (bmData->maybeInjectTargetedRestart()) {
					throw blob_manager_replaced();
				}
			}

			wait(assignFuture);

			if (assignment.previousFailure.present()) {
				// previous assign failed and this one succeeded
				--bmData->stats.blockedAssignments;
			}

			return Void();
		} else {
			ASSERT(!assignment.assign.present());
			ASSERT(assignment.revoke.present());

			RevokeBlobRangeRequest req;
			req.keyRange = KeyRangeRef(StringRef(req.arena, assignment.keyRange.begin),
			                           StringRef(req.arena, assignment.keyRange.end));
			req.managerEpoch = epoch;
			req.managerSeqno = seqNo;
			req.dispose = assignment.revoke.get().dispose;

			// if that worker isn't alive anymore, this is a noop
			if (bmData->workersById.count(workerID.get())) {
				wait(bmData->workersById[workerID.get()].revokeBlobRangeRequest.getReply(req));
			} else {
				return Void();
			}
		}
	} catch (Error& e) {
		if (assignment.previousFailure.present()) {
			// previous assign failed, consider it unblocked if it's not a retriable error
			--bmData->stats.blockedAssignments;
		}
		state Error e2 = e;
		if (e.code() == error_code_operation_cancelled) {
			throw;
		}
		if (e.code() == error_code_blob_manager_replaced) {
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.send(Void());
			}
			return Void();
		}
		if (e.code() == error_code_granule_assignment_conflict) {
			// Another blob worker already owns the range, don't retry.
			// And, if it was us that send the request to another worker for this range, this actor should have been
			// cancelled. So if it wasn't, it's likely that the conflict is from a new blob manager. Trigger the lock
			// check to make sure, and die if so.
			if (BM_DEBUG) {
				fmt::print("BM {0} got conflict assigning [{1} - {2}) to worker {3}, ignoring\n",
				           bmData->epoch,
				           assignment.keyRange.begin.printable(),
				           assignment.keyRange.end.printable(),
				           workerID.get().toString());
			}
			if (bmData->doLockCheck.canBeSet()) {
				bmData->doLockCheck.send(Void());
			}
			return Void();
		}

		if (e.code() != error_code_broken_promise && e.code() != error_code_no_more_servers &&
		    e.code() != error_code_blob_worker_full) {
			TraceEvent(SevWarn, "BlobManagerUnexpectedErrorDoRangeAssignment", bmData->id)
			    .error(e)
			    .detail("Epoch", bmData->epoch);
			ASSERT_WE_THINK(false);
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.sendError(e);
			}
			throw;
		}

		// this assign failed and we will retry, consider it blocked until it successfully retries
		if (assignment.isAssign) {
			++bmData->stats.blockedAssignments;
		}

		if (e.code() == error_code_blob_worker_full) {
			CODE_PROBE(true, "blob worker too full");
			ASSERT(assignment.isAssign);
			try {
				if (assignment.previousFailure.present() &&
				    assignment.previousFailure.get().second.code() == error_code_blob_worker_full) {
					// if previous assignment also failed due to blob_worker_full, multiple workers are full, so wait
					// even longer
					CODE_PROBE(true, "multiple blob workers too full");
					wait(delayJittered(10.0));
				} else {
					wait(delayJittered(1.0)); // wait a bit before retrying
				}
			} catch (Error& e) {
				--bmData->stats.blockedAssignments;
				throw;
			}
		}

		CODE_PROBE(true, "BM retrying range assign");

		// We use reliable delivery (getReply), so the broken_promise means the worker is dead, and we may need to retry
		// somewhere else
		if (assignment.isAssign) {
			if (BM_DEBUG) {
				fmt::print("BM got error {0} assigning range [{1} - {2}) to worker {3}, requeueing\n",
				           e2.name(),
				           assignment.keyRange.begin.printable(),
				           assignment.keyRange.end.printable(),
				           workerID.get().toString());
			}

			// re-send revoke to queue to handle range being un-assigned from that worker before the new one
			RangeAssignment revokeOld;
			revokeOld.isAssign = false;
			revokeOld.worker = workerID;
			revokeOld.keyRange = assignment.keyRange;
			revokeOld.revoke = RangeRevokeData(false);

			handleRangeAssign(bmData, revokeOld);

			// send assignment back to queue as is, clearing designated worker if present
			// if we failed to send continue to the worker we thought owned the shard, it should be retried
			// as a normal assign
			ASSERT(assignment.assign.present());
			assignment.assign.get().type = AssignRequestType::Normal;
			assignment.worker.reset();
			std::pair<UID, Error> failure = { workerID.get(), e2 };
			assignment.previousFailure = failure;
			handleRangeAssign(bmData, assignment);
		} else {
			if (BM_DEBUG) {
				fmt::print("BM got error revoking range [{0} - {1}) from worker",
				           assignment.keyRange.begin.printable(),
				           assignment.keyRange.end.printable());
			}

			if (assignment.revoke.get().dispose) {
				if (BM_DEBUG) {
					printf(", retrying for dispose\n");
				}
				// send assignment back to queue as is, clearing designated worker if present
				assignment.worker.reset();
				handleRangeAssign(bmData, assignment);
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

static bool handleRangeIsAssign(Reference<BlobManagerData> bmData, RangeAssignment assignment, int64_t seqNo) {
	// Ensure range isn't currently assigned anywhere, and there is only 1 intersecting range
	auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
	int count = 0;
	UID workerId;
	for (auto i = currentAssignments.begin(); i != currentAssignments.end(); ++i) {
		if (assignment.assign.get().type == AssignRequestType::Continue) {
			ASSERT(assignment.worker.present());
			if (i.range() != assignment.keyRange || i.cvalue() != assignment.worker.get()) {
				CODE_PROBE(true, "BM assignment out of date");
				if (BM_DEBUG) {
					fmt::print("Out of date re-assign for ({0}, {1}). Assignment must have changed while "
					           "checking split.\n  Reassign: [{2} - {3}): {4}\n  Existing: [{5} - {6}): {7}\n",
					           bmData->epoch,
					           seqNo,
					           assignment.keyRange.begin.printable(),
					           assignment.keyRange.end.printable(),
					           assignment.worker.get().toString().substr(0, 5),
					           i.begin().printable(),
					           i.end().printable(),
					           i.cvalue().toString().substr(0, 5));
				}
				return false;
			}
		}
		count++;
	}
	ASSERT(count == 1);

	bool forcePurging = bmData->isForcePurging(assignment.keyRange);

	if (forcePurging && assignment.previousFailure.present()) {
		--bmData->stats.blockedAssignments;
	}
	if (assignment.worker.present() && assignment.worker.get().isValid()) {
		if (BM_DEBUG) {
			fmt::print("BW {0} already chosen for seqno {1} in BM {2}\n",
			           assignment.worker.get().toString(),
			           seqNo,
			           bmData->id.toString());
		}
		workerId = assignment.worker.get();

		bmData->workerAssignments.insert(assignment.keyRange, workerId);

		// If we know about the worker and this is not a continue, then this is a new range for the worker
		if (assignment.assign.get().type == AssignRequestType::Continue) {
			// if it is a continue, don't cancel an in-flight re-assignment. Send to actor collection instead of
			// assignsInProgress
			bmData->addActor.send(doRangeAssignment(bmData, assignment, workerId, bmData->epoch, seqNo));
		} else {
			if (!forcePurging) {
				bmData->assignsInProgress.insert(assignment.keyRange,
				                                 doRangeAssignment(bmData, assignment, workerId, bmData->epoch, seqNo));
			}
			if (bmData->workerStats.count(workerId)) {
				bmData->workerStats[workerId].numGranulesAssigned += 1;
			}
		}
	} else {
		// Ensure the key boundaries are updated before we pick a worker
		bmData->workerAssignments.insert(assignment.keyRange, UID());
		ASSERT(assignment.assign.get().type != AssignRequestType::Continue);
		if (!forcePurging) {
			bmData->assignsInProgress.insert(
			    assignment.keyRange, doRangeAssignment(bmData, assignment, Optional<UID>(), bmData->epoch, seqNo));
		}
	}
	return true;
}

static bool handleRangeIsRevoke(Reference<BlobManagerData> bmData, RangeAssignment assignment, int64_t seqNo) {
	if (assignment.worker.present()) {
		// revoke this specific range from this specific worker. Either part of recovery or failing a worker
		if (bmData->workerStats.count(assignment.worker.get())) {
			bmData->workerStats[assignment.worker.get()].numGranulesAssigned -= 1;
		}
		// if this revoke matches the worker assignment state, mark the range as unassigned
		auto existingRange = bmData->workerAssignments.rangeContaining(assignment.keyRange.begin);
		if (existingRange.range() == assignment.keyRange && existingRange.cvalue() == assignment.worker.get()) {
			bmData->workerAssignments.insert(assignment.keyRange, UID());
		}
		bmData->addActor.send(doRangeAssignment(bmData, assignment, assignment.worker.get(), bmData->epoch, seqNo));
	} else {
		auto currentAssignments = bmData->workerAssignments.intersectingRanges(assignment.keyRange);
		for (auto& it : currentAssignments) {
			// ensure range doesn't truncate existing ranges
			if (it.begin() < assignment.keyRange.begin || it.end() > assignment.keyRange.end) {
				// the only case where this is ok is on startup when a BM is revoking old granules after reading
				// knownBlobRanges and seeing that some are no longer present.
				auto knownRanges = bmData->knownBlobRanges.intersectingRanges(it.range());
				bool inKnownBlobRanges = false;
				for (auto& r : knownRanges) {
					if (r.value()) {
						inKnownBlobRanges = true;
						break;
					}
				}
				bool forcePurging = bmData->isForcePurging(it.range());
				if (it.cvalue() != UID() || (inKnownBlobRanges && !forcePurging)) {
					fmt::print("Assignment [{0} - {1}): {2} truncates range [{3} - {4}) ({5}, {6})\n",
					           assignment.keyRange.begin.printable(),
					           assignment.keyRange.end.printable(),
					           it.cvalue().toString().substr(0, 5),
					           it.begin().printable(),
					           it.end().printable(),
					           inKnownBlobRanges,
					           forcePurging);
					// assert on condition again to make assertion failure better than "false"
					ASSERT(it.cvalue() == UID() && (!inKnownBlobRanges || forcePurging));
				}
			}

			// It is fine for multiple disjoint sub-ranges to have the same sequence number since they were part
			// of the same logical change

			if (bmData->workerStats.count(it.value())) {
				bmData->workerStats[it.value()].numGranulesAssigned -= 1;
			}

			// revoke the range for the worker that owns it, not the worker specified in the revoke
			bmData->addActor.send(doRangeAssignment(bmData, assignment, it.value(), bmData->epoch, seqNo));
		}
		bmData->workerAssignments.insert(assignment.keyRange, UID());
	}

	bmData->assignsInProgress.cancel(assignment.keyRange);
	return true;
}

static bool handleRangeAssign(Reference<BlobManagerData> bmData, RangeAssignment assignment) {
	int64_t seqNo = bmData->seqNo;
	bmData->seqNo++;

	// modify the in-memory assignment data structures, and send request off to worker
	if (assignment.isAssign) {
		return handleRangeIsAssign(bmData, assignment, seqNo);
	} else {
		return handleRangeIsRevoke(bmData, assignment, seqNo);
	}
}

ACTOR Future<Void> checkManagerLock(Transaction* tr, Reference<BlobManagerData> bmData) {
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

		throw blob_manager_replaced();
	}
	tr->addReadConflictRange(singleKeyRange(blobManagerEpochKey));
	tr->addWriteConflictRange(singleKeyRange(blobManagerEpochKey));

	return Void();
}

ACTOR Future<Void> checkManagerLock(Reference<ReadYourWritesTransaction> tr, Reference<BlobManagerData> bmData) {
	wait(checkManagerLock(&(tr->getTransaction()), bmData));
	return Void();
}

ACTOR Future<Void> writeInitialGranuleMapping(Reference<BlobManagerData> bmData, BlobGranuleSplitPoints splitPoints) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	// don't do too many in one transaction
	state int i = 0;
	state int transactionChunkSize = BUGGIFY ? deterministicRandom()->randomInt(2, 5) : 1000;
	while (i < splitPoints.keys.size() - 1) {
		CODE_PROBE(i > 0, "multiple transactions for large granule split");
		tr->reset();
		state int j = 0;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkManagerLock(tr, bmData));
				// Instead of doing a krmSetRange for each granule, because it does a read-modify-write, we do one
				// krmSetRange for the whole batch, and then just individual sets for each intermediate boundary This
				// does one read per transaction instead of N serial reads per transaction
				state int endIdx = std::min(i + transactionChunkSize, (int)(splitPoints.keys.size() - 1));
				wait(krmSetRange(tr,
				                 blobGranuleMappingKeys.begin,
				                 KeyRangeRef(splitPoints.keys[i], splitPoints.keys[endIdx]),
				                 blobGranuleMappingValueFor(UID())));
				for (j = 0; i + j < endIdx; j++) {
					if (splitPoints.boundaries.count(splitPoints.keys[i + j])) {
						tr->set(blobGranuleMergeBoundaryKeyFor(splitPoints.keys[i + j]),
						        blobGranuleMergeBoundaryValueFor(splitPoints.boundaries[splitPoints.keys[i + j]]));
					}
					tr->set(splitPoints.keys[i + j].withPrefix(blobGranuleMappingKeys.begin),
					        blobGranuleMappingValueFor(UID()));
				}
				wait(tr->commit());

				// Update BlobGranuleMergeBoundary in-memory state.
				for (int k = i; k < i + j; k++) {
					KeyRef beginKey = splitPoints.keys[k];
					if (splitPoints.boundaries.count(beginKey)) {
						bmData->mergeBoundaries[beginKey] = splitPoints.boundaries[beginKey];
					}
				}
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
				j = 0;
			}
		}
		i += j;
	}
	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}
	return Void();
}

ACTOR Future<Void> loadTenantMap(Reference<ReadYourWritesTransaction> tr, Reference<BlobManagerData> bmData) {
	state KeyBackedRangeResult<std::pair<TenantName, TenantMapEntry>> tenantResults;
	wait(store(tenantResults,
	           TenantMetadata::tenantMap().getRange(
	               tr, Optional<TenantName>(), Optional<TenantName>(), CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1)));
	ASSERT(tenantResults.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantResults.more);

	bmData->tenantData.addTenants(tenantResults.results);

	return Void();
}

ACTOR Future<Void> monitorTenants(Reference<BlobManagerData> bmData) {
	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(loadTenantMap(tr, bmData));

				state Future<Void> watchChange = tr->watch(TenantMetadata::lastTenantId().key);
				wait(tr->commit());
				wait(watchChange);
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}
}

// FIXME: better way to load tenant mapping?
ACTOR Future<Void> monitorClientRanges(Reference<BlobManagerData> bmData) {
	state Optional<Value> lastChangeKeyValue;
	state bool needToCoalesce = bmData->epoch > 1;
	state bool firstLoad = true;

	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

		if (BM_DEBUG) {
			printf("Blob manager checking for range updates\n");
		}
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// read change key at this point along with data
				state Optional<Value> ckvBegin = wait(tr->get(blobRangeChangeKey));

				state Arena ar;
				state RangeResult results = wait(krmGetRanges(tr,
				                                              blobRangeKeys.begin,
				                                              KeyRange(normalKeys),
				                                              CLIENT_KNOBS->TOO_MANY,
				                                              GetRangeLimits::BYTE_LIMIT_UNLIMITED));
				ASSERT_WE_THINK(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);
				if (results.more || results.size() >= CLIENT_KNOBS->TOO_MANY) {
					TraceEvent(SevError, "BlobManagerTooManyClientRanges", bmData->id)
					    .detail("Epoch", bmData->epoch)
					    .detail("ClientRanges", results.size() - 1);
					wait(delay(600));
					if (bmData->iAmReplaced.canBeSet()) {
						bmData->iAmReplaced.sendError(internal_error());
					}
					throw internal_error();
				}

				// TODO better way to do this!
				bmData->mergeHardBoundaries.clear();
				for (auto& it : results) {
					bmData->mergeHardBoundaries[it.key] = true;
				}
				ar.dependsOn(results.arena());

				VectorRef<KeyRangeRef> rangesToAdd;
				VectorRef<KeyRangeRef> rangesToRemove;
				updateClientBlobRanges(&bmData->knownBlobRanges, results, ar, &rangesToAdd, &rangesToRemove);

				if (needToCoalesce) {
					// recovery has granules instead of known ranges in here. We need to do so to identify any parts of
					// known client ranges the last manager didn't finish blob-ifying.
					// To coalesce the map, we simply override known ranges with the current DB ranges after computing
					// rangesToAdd + rangesToRemove
					needToCoalesce = false;

					for (int i = 0; i < results.size() - 1; i++) {
						bool active = results[i].value == blobRangeActive;
						bmData->knownBlobRanges.insert(KeyRangeRef(results[i].key, results[i + 1].key), active);
					}
				}

				state std::vector<Future<BlobGranuleSplitPoints>> splitFutures;
				// Divide new ranges up into equal chunks by using SS byte sample
				for (KeyRangeRef range : rangesToAdd) {
					TraceEvent("ClientBlobRangeAdded", bmData->id).detail("Range", range);
					// add client range as known "granule" until we determine initial split, in case a purge or
					// unblobbify comes in before we finish splitting

					// TODO can remove validation eventually
					auto r = bmData->workerAssignments.intersectingRanges(range);
					for (auto& it : r) {
						ASSERT(it.cvalue() == UID());
					}
					bmData->workerAssignments.insert(range, UID());

					// start initial split for range
					splitFutures.push_back(splitRange(bmData, range, false, true));
				}

				for (KeyRangeRef range : rangesToRemove) {
					TraceEvent("ClientBlobRangeRemoved", bmData->id).detail("Range", range);
					if (BM_DEBUG) {
						fmt::print(
						    "BM Got range to revoke [{0} - {1})\n", range.begin.printable(), range.end.printable());
					}

					RangeAssignment ra;
					ra.isAssign = false;
					ra.keyRange = range;
					ra.revoke = RangeRevokeData(true); // dispose=true
					handleRangeAssign(bmData, ra);
				}

				if (firstLoad) {
					bmData->loadedClientRanges.send(Void());
					firstLoad = false;
				}

				for (auto f : splitFutures) {
					state BlobGranuleSplitPoints splitPoints = wait(f);
					if (BM_DEBUG) {
						fmt::print("Split client range [{0} - {1}) into {2} ranges:\n",
						           splitPoints.keys[0].printable(),
						           splitPoints.keys[splitPoints.keys.size() - 1].printable(),
						           splitPoints.keys.size() - 1);
					}

					// Write to DB BEFORE sending assign requests, so that if manager dies before/during, new manager
					// picks up the same ranges
					wait(writeInitialGranuleMapping(bmData, splitPoints));

					for (int i = 0; i < splitPoints.keys.size() - 1; i++) {
						KeyRange range = KeyRange(KeyRangeRef(splitPoints.keys[i], splitPoints.keys[i + 1]));
						// only add the client range if this is the first BM or it's not already assigned
						if (BM_DEBUG) {
							fmt::print(
							    "    [{0} - {1})\n", range.begin.printable().c_str(), range.end.printable().c_str());
						}

						RangeAssignment ra;
						ra.isAssign = true;
						ra.keyRange = range;
						ra.assign = RangeAssignmentData(); // type=normal
						handleRangeAssign(bmData, ra);
					}
				}

				lastChangeKeyValue =
				    ckvBegin; // the version of the ranges we processed is the one read alongside the ranges

				// do a new transaction, check for change in change key, watch if none
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				state Future<Void> watchFuture;

				Optional<Value> ckvEnd = wait(tr->get(blobRangeChangeKey));

				if (ckvEnd == lastChangeKeyValue) {
					watchFuture = tr->watch(blobRangeChangeKey); // watch for change in key
					wait(tr->commit());
					if (BM_DEBUG) {
						printf("Blob manager done processing client ranges, awaiting update\n");
					}
				} else {
					watchFuture = Future<Void>(Void()); // restart immediately
				}

				wait(watchFuture);
				break;
			} catch (Error& e) {
				if (BM_DEBUG) {
					fmt::print("Blob manager got error looking for range updates {}\n", e.name());
				}
				wait(tr->onError(e));
			}
		}
	}
}

// split recursively in the middle to guarantee roughly equal splits across different parts of key space
static void downsampleSplit(const Standalone<VectorRef<KeyRef>>& splits,
                            Standalone<VectorRef<KeyRef>>& out,
                            int startIdx,
                            int endIdx,
                            int remaining) {
	ASSERT(endIdx - startIdx >= remaining);
	ASSERT(remaining >= 0);
	if (remaining == 0) {
		return;
	}
	if (endIdx - startIdx == remaining) {
		out.append(out.arena(), splits.begin() + startIdx, remaining);
	} else {
		int mid = (startIdx + endIdx) / 2;
		int startCount = (remaining - 1) / 2;
		int endCount = remaining - startCount - 1;
		// ensure no infinite recursion
		ASSERT(mid != endIdx);
		ASSERT(mid + 1 != startIdx);
		downsampleSplit(splits, out, startIdx, mid, startCount);
		out.push_back(out.arena(), splits[mid]);
		downsampleSplit(splits, out, mid + 1, endIdx, endCount);
	}
}

ACTOR Future<Void> reevaluateInitialSplit(Reference<BlobManagerData> bmData,
                                          UID currentWorkerId,
                                          KeyRange granuleRange,
                                          UID granuleID,
                                          int64_t epoch,
                                          int64_t seqno,
                                          Key proposedSplitKey) {
	CODE_PROBE(true, "BM re-evaluating initial split too big");
	if (BM_DEBUG) {
		fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big from {3} @ ({4}, {5})\n",
		           bmData->epoch,
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           currentWorkerId.toString().substr(0, 5),
		           epoch,
		           seqno);
		fmt::print("Proposed split (2):\n");
		fmt::print("    {0}\n", granuleRange.begin.printable());
		fmt::print("    {0}\n", proposedSplitKey.printable());
		fmt::print("    {0}\n", granuleRange.end.printable());
	}
	TraceEvent("BMCheckInitialSplitTooBig", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Granule", granuleRange)
	    .detail("ProposedSplitKey", proposedSplitKey);
	// calculate new split targets speculatively assuming split is too large and current worker still owns it
	ASSERT(granuleRange.begin < proposedSplitKey);
	ASSERT(proposedSplitKey < granuleRange.end);
	state Future<BlobGranuleSplitPoints> fSplitFirst =
	    splitRange(bmData, KeyRangeRef(granuleRange.begin, proposedSplitKey), false, true);
	state Future<BlobGranuleSplitPoints> fSplitSecond =
	    splitRange(bmData, KeyRangeRef(proposedSplitKey, granuleRange.end), false, true);

	state Standalone<VectorRef<KeyRef>> newRanges;

	BlobGranuleSplitPoints splitFirst = wait(fSplitFirst);
	ASSERT(splitFirst.keys.size() >= 2);
	ASSERT(splitFirst.keys.front() == granuleRange.begin);
	ASSERT(splitFirst.keys.back() == proposedSplitKey);
	for (int i = 0; i < splitFirst.keys.size(); i++) {
		newRanges.push_back_deep(newRanges.arena(), splitFirst.keys[i]);
	}

	BlobGranuleSplitPoints splitSecond = wait(fSplitSecond);
	ASSERT(splitSecond.keys.size() >= 2);
	ASSERT(splitSecond.keys.front() == proposedSplitKey);
	ASSERT(splitSecond.keys.back() == granuleRange.end);
	// i=1 to skip proposedSplitKey, since above already added it
	for (int i = 1; i < splitSecond.keys.size(); i++) {
		newRanges.push_back_deep(newRanges.arena(), splitSecond.keys[i]);
	}

	if (BM_DEBUG) {
		fmt::print("Re-evaluated split ({0}):\n", newRanges.size());
		for (auto& it : newRanges) {
			fmt::print("    {0}\n", it.printable());
		}
	}

	// redo key alignment on full set of split points
	// FIXME: only need to align propsedSplitKey in the middle
	state BlobGranuleSplitPoints finalSplit = wait(alignKeys(bmData, granuleRange, newRanges));

	ASSERT(finalSplit.keys.size() > 2);

	if (BM_DEBUG) {
		fmt::print("Aligned split ({0}):\n", finalSplit.keys.size());
		for (auto& it : finalSplit.keys) {
			fmt::print("    {0}{1}\n", it.printable(), finalSplit.boundaries.count(it) ? " *" : "");
		}
	}

	// Check lock to see if lock is still the specified epoch and seqno, and there are no files for the granule.
	// If either of these are false, some other worker now has the granule. if there are files, it already succeeded at
	// a split. if not, and it fails too, it will retry and get back here
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state Key lockKey = blobGranuleLockKeyFor(granuleRange);
	state bool retried = false;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			// make sure we're still manager when this transaction gets committed
			wait(checkManagerLock(tr, bmData));

			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr->getTransaction(), granuleRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Initial Split Re-evaluate stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleSplitReEvalCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", granuleRange);

				// destroy already created change feed from worker so it doesn't leak
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(granuleID),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      granuleRange));

				wait(tr->commit());

				return Void();
			}

			// this adds a read conflict range, so if another granule concurrently commits a file, we will retry and see
			// that
			KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
			RangeResult granuleFiles = wait(tr->getRange(range, 1));
			if (!granuleFiles.empty()) {
				CODE_PROBE(true, "split too big was eventually solved by another worker", probe::decoration::rare);
				if (BM_DEBUG) {
					fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big: solved by another worker\n",
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
				}
				return Void();
			}

			Optional<Value> prevLockValue = wait(tr->get(lockKey));
			ASSERT(prevLockValue.present());
			std::tuple<int64_t, int64_t, UID> prevOwner = decodeBlobGranuleLockValue(prevLockValue.get());
			int64_t prevOwnerEpoch = std::get<0>(prevOwner);
			int64_t prevOwnerSeqno = std::get<1>(prevOwner);
			UID prevGranuleID = std::get<2>(prevOwner);
			if (prevOwnerEpoch != epoch || prevOwnerSeqno != seqno || prevGranuleID != granuleID) {
				if (retried && prevOwnerEpoch == bmData->epoch && prevGranuleID == granuleID &&
				    prevOwnerSeqno == std::numeric_limits<int64_t>::max()) {
					// owner didn't change, last iteration of this transaction just succeeded but threw an error.
					CODE_PROBE(true, "split too big adjustment succeeded after retry");
					break;
				}
				CODE_PROBE(true, "split too big was since moved to another worker");
				if (BM_DEBUG) {
					fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big: moved to another worker\n",
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
					fmt::print("Epoch: Prev {0}, Cur {1}\n", prevOwnerEpoch, epoch);
					fmt::print("Seqno: Prev {0}, Cur {1}\n", prevOwnerSeqno, seqno);
					fmt::print("GranuleID: Prev {0}, Cur {1}\n",
					           prevGranuleID.toString().substr(0, 6),
					           granuleID.toString().substr(0, 6));
				}
				return Void();
			}

			if (prevOwnerEpoch > bmData->epoch) {
				if (BM_DEBUG) {
					fmt::print("BM {0} found a higher epoch {1} for granule lock of [{2} - {3})\n",
					           bmData->epoch,
					           prevOwnerEpoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
				}

				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}

			// The lock check above *should* handle this, but just be sure, also make sure that this granule wasn't
			// already split in the granule mapping
			RangeResult existingRanges = wait(
			    krmGetRanges(tr, blobGranuleMappingKeys.begin, granuleRange, 3, GetRangeLimits::BYTE_LIMIT_UNLIMITED));
			if (existingRanges.size() > 2 || existingRanges.more) {
				CODE_PROBE(true, "split too big was already re-split", probe::decoration::rare);
				if (BM_DEBUG) {
					fmt::print("BM {0} re-evaluating initial split [{1} - {2}) too big: already split\n",
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
					for (auto& it : existingRanges) {
						fmt::print("  {0}\n", it.key.printable());
					}
				}
				return Void();
			}

			// Set lock to max value for this manager, so other reassignments can't race with this transaction
			// and existing owner can't modify it further.
			tr->set(lockKey, blobGranuleLockValueFor(bmData->epoch, std::numeric_limits<int64_t>::max(), granuleID));

			// set new ranges
			state int i;
			for (i = 0; i < finalSplit.keys.size() - 1; i++) {
				wait(krmSetRange(tr,
				                 blobGranuleMappingKeys.begin,
				                 KeyRangeRef(finalSplit.keys[i], finalSplit.keys[i + 1]),
				                 blobGranuleMappingValueFor(UID())));
				if (finalSplit.boundaries.count(finalSplit.keys[i])) {
					tr->set(blobGranuleMergeBoundaryKeyFor(finalSplit.keys[i]),
					        blobGranuleMergeBoundaryValueFor(finalSplit.boundaries[finalSplit.keys[i]]));
				}
			}

			// Need to destroy the old change feed for the no longer needed feed, otherwise it will leak
			// This has to be a non-ryw transaction for the change feed destroy mutations to propagate properly
			// TODO: fix this better! (privatize change feed key clear)
			wait(updateChangeFeed(&tr->getTransaction(),
			                      granuleIDToCFKey(granuleID),
			                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
			                      granuleRange));

			retried = true;
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		wait(delay(0)); // should be cancelled
		ASSERT(false);
	}

	// transaction committed, send updated range assignments. Even if there is only one range still, we need to revoke
	// it and re-assign it to cancel the old granule and retry
	CODE_PROBE(true, "BM successfully changed initial split too big");
	RangeAssignment raRevoke;
	raRevoke.isAssign = false;
	raRevoke.keyRange = granuleRange;
	raRevoke.revoke = RangeRevokeData(false); // not a dispose
	handleRangeAssign(bmData, raRevoke);

	for (int i = 0; i < finalSplit.keys.size() - 1; i++) {
		// reassign new range and do handover of previous range
		RangeAssignment raAssignSplit;
		raAssignSplit.isAssign = true;
		raAssignSplit.keyRange = KeyRangeRef(finalSplit.keys[i], finalSplit.keys[i + 1]);
		raAssignSplit.assign = RangeAssignmentData();
		// don't care who this range gets assigned to
		handleRangeAssign(bmData, raAssignSplit);
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} Re-splitting initial range [{1} - {2}) into {3} granules done\n",
		           bmData->epoch,
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           finalSplit.keys.size() - 1);
	}

	return Void();
}

ACTOR Future<Void> maybeSplitRange(Reference<BlobManagerData> bmData,
                                   UID currentWorkerId,
                                   KeyRange granuleRange,
                                   UID granuleID,
                                   Version granuleStartVersion,
                                   bool writeHot,
                                   int64_t originalEpoch,
                                   int64_t originalSeqno) {
	if (bmData->isForcePurging(granuleRange)) {
		// ignore
		return Void();
	}
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

	// first get ranges to split
	state BlobGranuleSplitPoints splitPoints = wait(splitRange(bmData, granuleRange, writeHot, false));

	ASSERT(splitPoints.keys.size() >= 2);
	if (splitPoints.keys.size() == 2) {
		// not large enough to split, just reassign back to worker
		if (BM_DEBUG) {
			fmt::print("Not splitting existing range [{0} - {1}). Continuing assignment to {2}\n",
			           granuleRange.begin.printable(),
			           granuleRange.end.printable(),
			           currentWorkerId.toString());
		}

		// -1 because we are going to send the continue at seqNo, so the guard we are setting here needs to be greater
		// than whatever came before, but less than the continue seqno
		int64_t seqnoForEval = bmData->seqNo - 1;

		RangeAssignment raContinue;
		raContinue.isAssign = true;
		raContinue.worker = currentWorkerId;
		raContinue.keyRange = granuleRange;
		raContinue.assign = RangeAssignmentData(AssignRequestType::Continue); // continue assignment and re-snapshot
		bool reassignSuccess = handleRangeAssign(bmData, raContinue);

		// set updated boundary evaluation to avoid racing calls getting unblocked after here
		// We need to only revoke based on non-continue seqno, but ignore duplicate calls based on continue seqno
		if (reassignSuccess) {
			bmData->boundaryEvaluations.insert(
			    granuleRange,
			    BoundaryEvaluation(bmData->epoch, seqnoForEval, BoundaryEvalType::SPLIT, originalEpoch, originalSeqno));
		}

		return Void();
	}

	// Enforce max split fanout for performance reasons. This mainly happens when a blob worker is behind.
	if (splitPoints.keys.size() >=
	    SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 2) { // +2 because this is boundaries, so N keys would have N+1 bounaries.
		CODE_PROBE(true, "downsampling granule split because fanout too high");
		Standalone<VectorRef<KeyRef>> coalescedRanges;
		coalescedRanges.arena().dependsOn(splitPoints.keys.arena());
		coalescedRanges.push_back(coalescedRanges.arena(), splitPoints.keys.front());

		// since we include start + end boundaries here, only need maxSplitFanout-1 split boundaries to produce
		// maxSplitFanout granules
		downsampleSplit(
		    splitPoints.keys, coalescedRanges, 1, splitPoints.keys.size() - 1, SERVER_KNOBS->BG_MAX_SPLIT_FANOUT - 1);

		coalescedRanges.push_back(coalescedRanges.arena(), splitPoints.keys.back());
		ASSERT(coalescedRanges.size() == SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 1);
		if (BM_DEBUG) {
			fmt::print("Downsampled split [{0} - {1}) from {2} -> {3} granules\n",
			           granuleRange.begin.printable(),
			           granuleRange.end.printable(),
			           splitPoints.keys.size() - 1,
			           SERVER_KNOBS->BG_MAX_SPLIT_FANOUT);
		}

		// TODO probably do something better here?
		wait(store(splitPoints, alignKeys(bmData, granuleRange, coalescedRanges)));
		ASSERT(splitPoints.keys.size() <= SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 1);
	}

	ASSERT(granuleRange.begin == splitPoints.keys.front());
	ASSERT(granuleRange.end == splitPoints.keys.back());

	// Have to make set of granule ids deterministic across retries to not end up with extra UIDs in the split
	// state, which could cause recovery to fail and resources to not be cleaned up.
	// This entire transaction must be idempotent across retries for all splitting state
	state std::vector<UID> newGranuleIDs;
	newGranuleIDs.reserve(splitPoints.keys.size() - 1);
	for (int i = 0; i < splitPoints.keys.size() - 1; i++) {
		newGranuleIDs.push_back(deterministicRandom()->randomUniqueID());
	}

	if (BM_DEBUG) {
		fmt::print("Splitting range [{0} - {1}) into {2} granules:\n",
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           splitPoints.keys.size() - 1);
		for (int i = 0; i < splitPoints.keys.size(); i++) {
			fmt::print("    {0}:{1}{2}\n",
			           (i < newGranuleIDs.size() ? newGranuleIDs[i] : UID()).toString().substr(0, 6).c_str(),
			           splitPoints.keys[i].printable(),
			           splitPoints.boundaries.count(splitPoints.keys[i]) ? " *" : "");
		}
	}

	state Version splitVersion;

	// Need to split range. Persist intent to split and split metadata to DB BEFORE sending split assignments to blob
	// workers, so that nothing is lost on blob manager recovery
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			ASSERT(splitPoints.keys.size() > 2);

			// make sure we're still manager when this transaction gets committed
			wait(checkManagerLock(tr, bmData));
			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr->getTransaction(), granuleRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Split stopped because of force purge");
				TraceEvent("GranuleSplitCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", granuleRange);
				return Void();
			}

			// TODO can do this + lock in parallel
			// Read splitState to see if anything was committed instead of reading granule mapping because we don't want
			// to conflict with mapping changes/reassignments
			state RangeResult existingState =
			    wait(tr->getRange(blobGranuleSplitKeyRangeFor(granuleID), SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 2));
			ASSERT_WE_THINK(!existingState.more && existingState.size() <= SERVER_KNOBS->BG_MAX_SPLIT_FANOUT + 1);
			// maybe someone decreased the knob, we should gracefully handle it not in simulation
			if (existingState.more || existingState.size() > SERVER_KNOBS->BG_MAX_SPLIT_FANOUT) {
				RangeResult tryAgain = wait(tr->getRange(blobGranuleSplitKeyRangeFor(granuleID), 10000));
				ASSERT(!tryAgain.more);
				existingState = tryAgain;
			}
			if (!existingState.empty()) {
				// Something was previously committed, we must go with that decision.
				// Read its boundaries and override our planned split boundaries
				CODE_PROBE(true, "Overriding split ranges with existing ones from DB");
				RangeResult existingBoundaries =
				    wait(tr->getRange(KeyRangeRef(granuleRange.begin.withPrefix(blobGranuleMappingKeys.begin),
				                                  keyAfter(granuleRange.end).withPrefix(blobGranuleMappingKeys.begin)),
				                      existingState.size() + 2));
				// +2 because this is boundaries and existingState was granules, and to ensure it doesn't set more
				ASSERT(!existingBoundaries.more);
				ASSERT(existingBoundaries.size() == existingState.size() + 1);
				splitPoints.keys.clear();
				splitPoints.keys.arena().dependsOn(existingBoundaries.arena());
				for (auto& it : existingBoundaries) {
					splitPoints.keys.push_back(splitPoints.keys.arena(),
					                           it.key.removePrefix(blobGranuleMappingKeys.begin));
				}
				// We don't care about splitPoints.boundaries as they are already persisted.
				splitPoints.boundaries.clear();
				ASSERT(splitPoints.keys.front() == granuleRange.begin);
				ASSERT(splitPoints.keys.back() == granuleRange.end);
				if (BM_DEBUG) {
					fmt::print("Replaced old range splits for [{0} - {1}) with {2}:\n",
					           granuleRange.begin.printable(),
					           granuleRange.end.printable(),
					           splitPoints.keys.size() - 1);
					for (int i = 0; i < splitPoints.keys.size(); i++) {
						fmt::print("    {}\n", splitPoints.keys[i].printable());
					}
				}
				break;
			}

			// acquire lock for old granule to make sure nobody else modifies it
			state Key lockKey = blobGranuleLockKeyFor(granuleRange);
			Optional<Value> lockValue = wait(tr->get(lockKey));
			ASSERT(lockValue.present());
			std::tuple<int64_t, int64_t, UID> prevGranuleLock = decodeBlobGranuleLockValue(lockValue.get());
			int64_t ownerEpoch = std::get<0>(prevGranuleLock);

			if (ownerEpoch > bmData->epoch) {
				if (BM_DEBUG) {
					fmt::print("BM {0} found a higher epoch {1} than {2} for granule lock of [{3} - {4})\n",
					           bmData->epoch,
					           ownerEpoch,
					           bmData->epoch,
					           granuleRange.begin.printable(),
					           granuleRange.end.printable());
				}

				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}

			// Set lock to max value for this manager, so other reassignments can't race with this transaction
			// and existing owner can't modify it further.
			// Merging makes lock go backwards if we later merge other granules back to this same range, but it is fine
			tr->set(lockKey,
			        blobGranuleLockValueFor(
			            bmData->epoch, std::numeric_limits<int64_t>::max(), std::get<2>(prevGranuleLock)));

			// get last delta file version written, to make that the split version
			RangeResult lastDeltaFile =
			    wait(tr->getRange(blobGranuleFileKeyRangeFor(granuleID), 1, Snapshot::False, Reverse::True));
			ASSERT(lastDeltaFile.size() == 1);
			std::tuple<UID, Version, uint8_t> k = decodeBlobGranuleFileKey(lastDeltaFile[0].key);
			ASSERT(std::get<0>(k) == granuleID);
			ASSERT(std::get<2>(k) == 'D');
			splitVersion = std::get<1>(k);

			if (BM_DEBUG) {
				fmt::print("BM {0} found version {1} for splitting [{2} - {3})\n",
				           bmData->epoch,
				           splitVersion,
				           granuleRange.begin.printable(),
				           granuleRange.end.printable());
			}

			// set up splits in granule mapping, but point each part to the old owner (until they get reassigned)
			state int i;
			for (i = 0; i < splitPoints.keys.size() - 1; i++) {
				KeyRangeRef splitRange(splitPoints.keys[i], splitPoints.keys[i + 1]);

				// Record split state.
				Key splitKey = blobGranuleSplitKeyFor(granuleID, newGranuleIDs[i]);

				tr->atomicOp(splitKey,
				             blobGranuleSplitValueFor(BlobGranuleSplitState::Initialized),
				             MutationRef::SetVersionstampedValue);

				// Update BlobGranuleMergeBoundary.
				if (splitPoints.boundaries.count(splitRange.begin)) {
					tr->set(blobGranuleMergeBoundaryKeyFor(splitRange.begin),
					        blobGranuleMergeBoundaryValueFor(splitPoints.boundaries[splitRange.begin]));
				}

				// History.
				Key historyKey = blobGranuleHistoryKeyFor(splitRange, splitVersion);

				Standalone<BlobGranuleHistoryValue> historyValue;
				historyValue.granuleID = newGranuleIDs[i];
				historyValue.parentBoundaries.push_back(historyValue.arena(), granuleRange.begin);
				historyValue.parentBoundaries.push_back(historyValue.arena(), granuleRange.end);
				historyValue.parentVersions.push_back(historyValue.arena(), granuleStartVersion);

				tr->set(historyKey, blobGranuleHistoryValueFor(historyValue));

				// split the assignment but still pointing to the same worker
				// FIXME: could pick new random workers here, they'll get overridden shortly unless the BM immediately
				// restarts
				wait(krmSetRange(tr,
				                 blobGranuleMappingKeys.begin,
				                 KeyRangeRef(splitPoints.keys[i], splitPoints.keys[i + 1]),
				                 blobGranuleMappingValueFor(currentWorkerId)));
			}

			wait(tr->commit());

			if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			// Update BlobGranuleMergeBoundary in-memory state.
			for (auto it = splitPoints.boundaries.begin(); it != splitPoints.boundaries.end(); it++) {
				bmData->mergeBoundaries[it->first] = it->second;
			}

			break;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			if (BM_DEBUG) {
				fmt::print("BM {0} Persisting granule split got error {1}\n", bmData->epoch, e.name());
			}
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
		fmt::print("Splitting range [{0} - {1}) into {2} granules @ {3} done, sending assignments:\n",
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           splitPoints.keys.size() - 1,
		           splitVersion);
	}

	++bmData->stats.granuleSplits;
	if (writeHot) {
		++bmData->stats.granuleWriteHotSplits;
	}

	int64_t seqnoForEval = bmData->seqNo;

	// transaction committed, send range assignments
	// range could have been moved since split eval started, so just revoke from whoever has it
	RangeAssignment raRevoke;
	raRevoke.isAssign = false;
	raRevoke.keyRange = granuleRange;
	raRevoke.revoke = RangeRevokeData(false); // not a dispose
	handleRangeAssign(bmData, raRevoke);

	for (int i = 0; i < splitPoints.keys.size() - 1; i++) {
		// reassign new range and do handover of previous range
		RangeAssignment raAssignSplit;
		raAssignSplit.isAssign = true;
		raAssignSplit.keyRange = KeyRangeRef(splitPoints.keys[i], splitPoints.keys[i + 1]);
		raAssignSplit.assign = RangeAssignmentData();
		// don't care who this range gets assigned to
		handleRangeAssign(bmData, raAssignSplit);
	}

	if (BM_DEBUG) {
		fmt::print("Splitting range [{0} - {1}) into {2} granules @ {3} got assignments processed\n",
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           splitPoints.keys.size() - 1,
		           splitVersion);
	}

	// set updated boundary evaluation to avoid racing calls getting unblocked after here
	bmData->boundaryEvaluations.insert(
	    granuleRange,
	    BoundaryEvaluation(bmData->epoch, seqnoForEval, BoundaryEvalType::SPLIT, originalEpoch, originalSeqno));

	return Void();
}

// read mapping from db to handle any in flight granules or other issues
// Forces all granules in the specified key range to flush data to blob up to the specified version. This is required
// for executing a merge.
ACTOR Future<bool> forceGranuleFlush(Reference<BlobManagerData> bmData,
                                     UID mergeGranuleID,
                                     KeyRange keyRange,
                                     Version version) {
	state Transaction tr(bmData->db);
	state KeyRange currentRange = keyRange;

	if (BM_DEBUG) {
		fmt::print(
		    "Flushing Granules [{0} - {1}) @ {2}\n", keyRange.begin.printable(), keyRange.end.printable(), version);
	}

	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		if (currentRange.begin == currentRange.end) {
			break;
		}
		try {
			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr, keyRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Granule flush stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleFlushCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("KeyRange", keyRange);

				// destroy already created change feed from earlier so it doesn't leak
				wait(updateChangeFeed(
				    &tr, granuleIDToCFKey(mergeGranuleID), ChangeFeedStatus::CHANGE_FEED_DESTROY, keyRange));

				wait(tr.commit());
				return false;
			}

			// TODO KNOB
			state RangeResult blobGranuleMapping = wait(krmGetRanges(
			    &tr, blobGranuleMappingKeys.begin, currentRange, 64, GetRangeLimits::BYTE_LIMIT_UNLIMITED));

			state int i = 0;
			state std::vector<Future<ErrorOr<Void>>> flushes;

			for (; i < blobGranuleMapping.size() - 1; i++) {
				if (!blobGranuleMapping[i].value.size()) {
					if (BM_DEBUG) {
						fmt::print("ERROR: No valid granule data for range [{1} - {2}) \n",
						           blobGranuleMapping[i].key.printable(),
						           blobGranuleMapping[i + 1].key.printable());
					}
					// range isn't force purged because of above check, so flush was for invalid range
					throw blob_granule_transaction_too_old();
				}

				state UID workerId = decodeBlobGranuleMappingValue(blobGranuleMapping[i].value);
				if (workerId == UID()) {
					if (BM_DEBUG) {
						fmt::print("ERROR: Invalid Blob Worker ID for range [{1} - {2}) \n",
						           blobGranuleMapping[i].key.printable(),
						           blobGranuleMapping[i + 1].key.printable());
					}
					// range isn't force purged because of above check, so flush was for invalid range
					throw blob_granule_transaction_too_old();
				}

				if (!tr.trState->cx->blobWorker_interf.count(workerId)) {
					Optional<Value> workerInterface = wait(tr.get(blobWorkerListKeyFor(workerId)));
					// from the time the mapping was read from the db, the associated blob worker
					// could have died and so its interface wouldn't be present as part of the blobWorkerList
					// we persist in the db.
					if (workerInterface.present()) {
						tr.trState->cx->blobWorker_interf[workerId] = decodeBlobWorkerListValue(workerInterface.get());
					} else {
						if (BM_DEBUG) {
							fmt::print("ERROR: Worker  for range [{1} - {2}) does not exist!\n",
							           workerId.toString().substr(0, 5),
							           blobGranuleMapping[i].key.printable(),
							           blobGranuleMapping[i + 1].key.printable());
						}
						break;
					}
				}

				if (BM_DEBUG) {
					fmt::print("Flushing range [{0} - {1}) from worker {2}!\n",
					           blobGranuleMapping[i].key.printable(),
					           blobGranuleMapping[i + 1].key.printable(),
					           workerId.toString().substr(0, 5));
				}

				KeyRangeRef range(blobGranuleMapping[i].key, blobGranuleMapping[i + 1].key);
				Future<ErrorOr<Void>> flush =
				    tr.trState->cx->blobWorker_interf[workerId].flushGranuleRequest.tryGetReply(
				        FlushGranuleRequest(bmData->epoch, range, version));
				flushes.push_back(flush);
			}
			// wait for each flush, if it has an error, retry from there if it is a retriable error
			state int j = 0;
			for (; j < flushes.size(); j++) {
				try {
					ErrorOr<Void> result = wait(flushes[j]);
					if (result.isError()) {
						throw result.getError();
					}
					if (BM_DEBUG) {
						fmt::print("Flushing range [{0} - {1}) complete!\n",
						           blobGranuleMapping[j].key.printable(),
						           blobGranuleMapping[j + 1].key.printable());
					}
				} catch (Error& e) {
					if (e.code() == error_code_wrong_shard_server || e.code() == error_code_request_maybe_delivered ||
					    e.code() == error_code_broken_promise || e.code() == error_code_connection_failed) {
						// re-read range and retry from failed req
						i = j;
						break;
					} else {
						if (BM_DEBUG) {
							fmt::print("ERROR: BM {0} Error flushing range [{1} - {2}): {3}!\n",
							           bmData->epoch,
							           blobGranuleMapping[j].key.printable(),
							           blobGranuleMapping[j + 1].key.printable(),
							           e.name());
						}
						throw;
					}
				}
			}
			if (i < blobGranuleMapping.size() - 1) {
				// a request failed, retry from there after a sleep
				currentRange = KeyRangeRef(blobGranuleMapping[i].key, currentRange.end);
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			} else if (blobGranuleMapping.more) {
				// no requests failed but there is more to read, continue reading
				currentRange = KeyRangeRef(blobGranuleMapping.back().key, currentRange.end);
			} else {
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (BM_DEBUG) {
		fmt::print("Flushing Granules [{0} - {1}) @ {2} Complete!\n",
		           keyRange.begin.printable(),
		           keyRange.end.printable(),
		           version);
	}

	return true;
}

// Persist the merge intent for this merge in the database. Once this transaction commits, the merge is in progress. It
// cannot be aborted, and must be completed.
ACTOR Future<std::pair<UID, Version>> persistMergeGranulesStart(Reference<BlobManagerData> bmData,
                                                                KeyRange mergeRange,
                                                                std::vector<UID> parentGranuleIDs,
                                                                std::vector<Key> parentGranuleRanges,
                                                                std::vector<Version> parentGranuleStartVersions) {
	state UID mergeGranuleID = deterministicRandom()->randomUniqueID();
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkManagerLock(tr, bmData));

			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr->getTransaction(), mergeRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Merge start stopped because of force purge", probe::decoration::rare);
				TraceEvent("GranuleMergeStartCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", mergeRange);

				// destroy already created change feed from earlier so it doesn't leak
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(mergeGranuleID),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      mergeRange));

				wait(tr->commit());

				bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
				bmData->activeGranuleMerges.coalesce(mergeRange.begin);

				// TODO better error?
				return std::pair(UID(), invalidVersion);
			}
			// FIXME: extra safeguard: check that granuleID of active lock == parentGranuleID for each parent, abort
			// merge if so

			tr->atomicOp(
			    blobGranuleMergeKeyFor(mergeGranuleID),
			    blobGranuleMergeValueFor(mergeRange, parentGranuleIDs, parentGranuleRanges, parentGranuleStartVersions),
			    MutationRef::SetVersionstampedValue);

			wait(updateChangeFeed(
			    tr, granuleIDToCFKey(mergeGranuleID), ChangeFeedStatus::CHANGE_FEED_CREATE, mergeRange));

			wait(tr->commit());

			if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			Version mergeVersion = tr->getCommittedVersion();
			if (BM_DEBUG) {
				fmt::print("Granule merge intent persisted [{0} - {1}): {2} @ {3}!\n",
				           mergeRange.begin.printable(),
				           mergeRange.end.printable(),
				           mergeGranuleID.shortString().substr(0, 6),
				           mergeVersion);
			}

			// update merge version in boundary evals so racing splits can continue if necessary
			auto mergeInProgress = bmData->activeGranuleMerges.rangeContaining(mergeRange.begin);
			if (BM_DEBUG) {
				fmt::print("Updating merge in progress [{0} - {1}) to merge version {2}!\n",
				           mergeInProgress.begin().printable(),
				           mergeInProgress.end().printable(),
				           mergeVersion);
			}
			ASSERT(mergeInProgress.begin() == mergeRange.begin);
			ASSERT(mergeInProgress.end() == mergeRange.end);
			ASSERT(mergeInProgress.cvalue() == 0);
			mergeInProgress.value() = mergeVersion;

			return std::pair(mergeGranuleID, mergeVersion);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Persists the merge being complete in the database by clearing the merge intent. Once this transaction commits, the
// merge is considered completed.
ACTOR Future<bool> persistMergeGranulesDone(Reference<BlobManagerData> bmData,
                                            UID mergeGranuleID,
                                            KeyRange mergeRange,
                                            Version mergeVersion,
                                            std::vector<UID> parentGranuleIDs,
                                            std::vector<Key> parentGranuleRanges,
                                            std::vector<Version> parentGranuleStartVersions) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	// pick worker that has part of old range, it will soon get overridden anyway
	state UID tmpWorkerId;
	auto ranges = bmData->workerAssignments.intersectingRanges(mergeRange);
	for (auto& it : ranges) {
		if (it.cvalue() != UID()) {
			tmpWorkerId = it.cvalue();
			break;
		}
	}
	if (tmpWorkerId == UID()) {
		CODE_PROBE(true, "All workers dead right now", probe::decoration::rare);
		while (bmData->workersById.empty()) {
			wait(bmData->recruitingStream.onChange() || bmData->foundBlobWorkers.getFuture());
		}
		tmpWorkerId = bmData->workersById.begin()->first;
	}
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			wait(checkManagerLock(tr, bmData));

			ForcedPurgeState purgeState = wait(getForcePurgedState(&tr->getTransaction(), mergeRange));
			if (purgeState != ForcedPurgeState::NonePurged) {
				CODE_PROBE(true, "Merge finish stopped because of force purge");
				TraceEvent("GranuleMergeCancelledForcePurge", bmData->id)
				    .detail("Epoch", bmData->epoch)
				    .detail("GranuleRange", mergeRange);

				// destroy already created change feed from earlier so it doesn't leak
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(mergeGranuleID),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      mergeRange));

				// TODO could also delete history entry here

				wait(tr->commit());

				bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
				bmData->activeGranuleMerges.coalesce(mergeRange.begin);

				return false;
			}

			tr->clear(blobGranuleMergeKeyFor(mergeGranuleID));

			state int parentIdx;
			// TODO: could parallelize these
			for (parentIdx = 0; parentIdx < parentGranuleIDs.size(); parentIdx++) {
				KeyRange parentRange(KeyRangeRef(parentGranuleRanges[parentIdx], parentGranuleRanges[parentIdx + 1]));
				state Key lockKey = blobGranuleLockKeyFor(parentRange);
				state Future<Optional<Value>> oldLockFuture = tr->get(lockKey);

				// Clear existing merge boundaries.
				tr->clear(blobGranuleMergeBoundaryKeyFor(parentRange.begin));

				// This has to be a non-ryw transaction for the change feed destroy mutations to propagate properly
				// TODO: fix this better! (privatize change feed key clear)
				wait(updateChangeFeed(&tr->getTransaction(),
				                      granuleIDToCFKey(parentGranuleIDs[parentIdx]),
				                      ChangeFeedStatus::CHANGE_FEED_DESTROY,
				                      parentRange));
				if (BM_DEBUG) {
					fmt::print("Granule merge destroying CF {0} ({1})!\n",
					           parentGranuleIDs[parentIdx].shortString().substr(0, 6),
					           granuleIDToCFKey(parentGranuleIDs[parentIdx]).printable());
				}

				Optional<Value> oldLock = wait(oldLockFuture);
				ASSERT(oldLock.present());
				auto prevLock = decodeBlobGranuleLockValue(oldLock.get());
				// Set lock to max value for this manager, so other reassignments can't race with this transaction
				// and existing owner can't modify it further.
				// Merging makes lock go backwards if we later split another granule back to this same range, but it is
				// fine and handled in the blob worker
				tr->set(
				    lockKey,
				    blobGranuleLockValueFor(bmData->epoch, std::numeric_limits<int64_t>::max(), std::get<2>(prevLock)));
			}

			tr->clear(KeyRangeRef(keyAfter(blobGranuleMergeBoundaryKeyFor(mergeRange.begin)),
			                      blobGranuleMergeBoundaryKeyFor(mergeRange.end)));

			// either set initial lock value, or re-set it if it was set to (epoch, <max>) in a previous split
			int64_t seqNo = bmData->seqNo++;
			tr->set(blobGranuleLockKeyFor(mergeRange), blobGranuleLockValueFor(bmData->epoch, seqNo, mergeGranuleID));

			// persist history entry
			Key historyKey = blobGranuleHistoryKeyFor(mergeRange, mergeVersion);

			Standalone<BlobGranuleHistoryValue> historyValue;
			historyValue.granuleID = mergeGranuleID;
			for (parentIdx = 0; parentIdx < parentGranuleIDs.size(); parentIdx++) {
				historyValue.parentBoundaries.push_back(historyValue.arena(), parentGranuleRanges[parentIdx]);
				historyValue.parentVersions.push_back(historyValue.arena(), parentGranuleStartVersions[parentIdx]);
			}
			historyValue.parentBoundaries.push_back(historyValue.arena(), parentGranuleRanges.back());

			tr->set(historyKey, blobGranuleHistoryValueFor(historyValue));

			wait(krmSetRange(tr, blobGranuleMappingKeys.begin, mergeRange, blobGranuleMappingValueFor(tmpWorkerId)));
			wait(tr->commit());

			// Update in-memory mergeBoundaries map.
			for (parentIdx = 1; parentIdx < parentGranuleIDs.size(); parentIdx++) {
				bmData->mergeBoundaries.erase(parentGranuleRanges[parentIdx]);
			}

			if (BM_DEBUG) {
				fmt::print("Granule merge intent cleared [{0} - {1}): {2} @ {3} (cv={4})\n",
				           mergeRange.begin.printable(),
				           mergeRange.end.printable(),
				           mergeGranuleID.shortString().substr(0, 6),
				           mergeVersion,
				           tr->getCommittedVersion());
			}
			CODE_PROBE(true, "Granule merge complete");

			if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
				wait(delay(0)); // should be cancelled
				ASSERT(false);
			}

			return true;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// This is the idempotent function that executes a granule merge once the initial merge intent has been persisted.
ACTOR Future<Void> finishMergeGranules(Reference<BlobManagerData> bmData,
                                       UID mergeGranuleID,
                                       KeyRange mergeRange,
                                       Version mergeVersion,
                                       std::vector<UID> parentGranuleIDs,
                                       std::vector<Key> parentGranuleRanges,
                                       std::vector<Version> parentGranuleStartVersions) {
	++bmData->stats.activeMerges;

	// wait for BM to be fully recovered and have loaded hard boundaries before starting actual merges
	wait(bmData->doneRecovering.getFuture());
	wait(bmData->loadedClientRanges.getFuture());
	wait(delay(0));

	// Assert that none of the subsequent granules are hard boundaries.
	if (g_network->isSimulated()) {
		for (int i = 1; i < parentGranuleRanges.size() - 1; i++) {
			ASSERT(!bmData->mergeHardBoundaries.count(parentGranuleRanges[i]));
		}
	}

	// force granules to persist state up to mergeVersion
	bool successFlush = wait(forceGranuleFlush(bmData, mergeGranuleID, mergeRange, mergeVersion));
	if (!successFlush) {
		bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
		bmData->activeGranuleMerges.coalesce(mergeRange.begin);
		--bmData->stats.activeMerges;
		return Void();
	}

	// update state and clear merge intent
	bool successFinish = wait(persistMergeGranulesDone(bmData,
	                                                   mergeGranuleID,
	                                                   mergeRange,
	                                                   mergeVersion,
	                                                   parentGranuleIDs,
	                                                   parentGranuleRanges,
	                                                   parentGranuleStartVersions));
	if (!successFinish) {
		bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
		bmData->activeGranuleMerges.coalesce(mergeRange.begin);
		--bmData->stats.activeMerges;
		return Void();
	}

	int64_t seqnoForEval = bmData->seqNo;

	// revoke old ranges and assign new range
	RangeAssignment revokeOld;
	revokeOld.isAssign = false;
	revokeOld.keyRange = mergeRange;
	revokeOld.revoke = RangeRevokeData(false);
	handleRangeAssign(bmData, revokeOld);

	RangeAssignment assignNew;
	assignNew.isAssign = true;
	assignNew.keyRange = mergeRange;
	assignNew.assign = RangeAssignmentData(); // not a continue
	handleRangeAssign(bmData, assignNew);

	bmData->activeGranuleMerges.insert(mergeRange, invalidVersion);
	bmData->activeGranuleMerges.coalesce(mergeRange.begin);

	bmData->boundaryEvaluations.insert(mergeRange,
	                                   BoundaryEvaluation(bmData->epoch, seqnoForEval, BoundaryEvalType::MERGE, 0, 0));
	bmData->setMergeCandidate(mergeRange, MergeCandidateMerging);

	--bmData->stats.activeMerges;

	return Void();
}

ACTOR Future<Void> doMerge(Reference<BlobManagerData> bmData,
                           KeyRange mergeRange,
                           std::vector<std::tuple<UID, KeyRange, Version>> toMerge) {
	// switch to format persist merge wants
	state std::vector<UID> ids;
	state std::vector<Key> ranges;
	state std::vector<Version> startVersions;
	for (auto& it : toMerge) {
		ids.push_back(std::get<0>(it));
		ranges.push_back(std::get<1>(it).begin);
		startVersions.push_back(std::get<2>(it));
	}
	ranges.push_back(std::get<1>(toMerge.back()).end);

	++bmData->stats.granuleMerges;

	try {
		std::pair<UID, Version> persistMerge =
		    wait(persistMergeGranulesStart(bmData, mergeRange, ids, ranges, startVersions));
		if (persistMerge.second == invalidVersion) {
			// cancelled because of force purge

			return Void();
		}
		wait(finishMergeGranules(
		    bmData, persistMerge.first, mergeRange, persistMerge.second, ids, ranges, startVersions));
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled || e.code() == error_code_blob_manager_replaced) {
			throw;
		}
		TraceEvent(SevError, "UnexpectedErrorGranuleMerge").error(e).detail("Range", mergeRange);
		throw e;
	}
}

// Needs to not be an actor to run synchronously for the race checking.
// Technically this could just be the first part of doMerge, but this guarantees no waits happen for the checks before
// the logic starts
static void attemptStartMerge(Reference<BlobManagerData> bmData,
                              const std::vector<std::tuple<UID, KeyRange, Version>>& toMerge) {
	if (toMerge.size() < 2) {
		return;
	}
	// TODO REMOVE validation eventually
	for (int i = 0; i < toMerge.size() - 1; i++) {
		ASSERT(std::get<1>(toMerge[i]).end == std::get<1>(toMerge[i + 1]).begin);
	}
	KeyRange mergeRange(KeyRangeRef(std::get<1>(toMerge.front()).begin, std::get<1>(toMerge.back()).end));
	// merge/merge races should not be possible because granuleMergeChecker should only start attemptMerges() for
	// disjoint ranges, and merge candidate is not updated if it is already in the state MergeCandidateMerging
	ASSERT(!bmData->isMergeActive(mergeRange));
	// Check to avoid races where a split eval came in while merge was evaluating. This also effectively checks
	// boundaryEvals because they're both updated before maybeSplitRange is called. This handles split/merge races.
	auto reCheckMergeCandidates = bmData->mergeCandidates.intersectingRanges(mergeRange);
	for (auto it : reCheckMergeCandidates) {
		if (!it->cvalue().mergeEligible()) {
			CODE_PROBE(true,
			           "granule no longer merge candidate after checking metrics, aborting merge",
			           probe::decoration::rare);
			return;
		}
	}

	if (bmData->isForcePurging(mergeRange)) {
		// ignore
		return;
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} Starting merge of [{1} - {2}) ({3})\n",
		           bmData->epoch,
		           mergeRange.begin.printable(),
		           mergeRange.end.printable(),
		           toMerge.size());
	}
	CODE_PROBE(true, "Doing granule merge");
	bmData->activeGranuleMerges.insert(mergeRange, 0);
	bmData->setMergeCandidate(mergeRange, MergeCandidateMerging);
	// Now, after setting activeGranuleMerges, we have committed to doing the merge, so any subsequent split eval for
	// any of the ranges will be ignored. This handles merge/split races.
	bmData->addActor.send(doMerge(bmData, mergeRange, toMerge));
}

// Greedily merges any consecutive 2+ granules in a row that are mergeable
ACTOR Future<Void> attemptMerges(Reference<BlobManagerData> bmData,
                                 std::vector<std::tuple<UID, KeyRange, Version>> candidates) {
	ASSERT(candidates.size() >= 2);

	// TODO REMOVE validation eventually
	for (int i = 0; i < candidates.size() - 1; i++) {
		ASSERT(std::get<1>(candidates[i]).end == std::get<1>(candidates[i + 1]).begin);
	}
	CODE_PROBE(true, "Candidate ranges to merge");
	wait(bmData->concurrentMergeChecks.take());
	state FlowLock::Releaser holdingLock(bmData->concurrentMergeChecks);

	// start merging any set of 2+ consecutive granules that can be merged
	state int64_t currentBytes = 0;
	// large keys can cause a large number of granules in the merge to exceed the maximum value size
	state int currentKeySumBytes = 0;
	state std::vector<std::tuple<UID, KeyRange, Version>> currentCandidates;
	state int i;
	for (i = 0; i < candidates.size(); i++) {
		StorageMetrics metrics =
		    wait(bmData->db->getStorageMetrics(std::get<1>(candidates[i]), CLIENT_KNOBS->TOO_MANY));

		if (metrics.bytes >= SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES ||
		    metrics.bytesPerKSecond >= SERVER_KNOBS->SHARD_MIN_BYTES_PER_KSEC) {
			// This granule cannot be merged with any neighbors.
			// If current candidates up to here can be merged, merge them and skip over this one
			attemptStartMerge(bmData, currentCandidates);
			currentCandidates.clear();
			currentBytes = 0;
			currentKeySumBytes = 0;
			continue;
		}

		// if the current window is already at the maximum merge size, or adding this granule would push the window over
		// the edge, merge the existing candidates if possible
		ASSERT(currentCandidates.size() <= SERVER_KNOBS->BG_MAX_MERGE_FANIN);
		if (currentCandidates.size() == SERVER_KNOBS->BG_MAX_MERGE_FANIN ||
		    currentBytes + metrics.bytes > SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES ||
		    currentKeySumBytes >= CLIENT_KNOBS->VALUE_SIZE_LIMIT / 2) {
			ASSERT(currentBytes <= SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES);
			CODE_PROBE(currentKeySumBytes >= CLIENT_KNOBS->VALUE_SIZE_LIMIT / 2, "merge early because of key size");
			attemptStartMerge(bmData, currentCandidates);
			currentCandidates.clear();
			currentBytes = 0;
			currentKeySumBytes = 0;
		}

		// add this granule to the window
		if (currentCandidates.empty()) {
			currentKeySumBytes += std::get<1>(candidates[i]).begin.size();
		}
		currentKeySumBytes += std::get<1>(candidates[i]).end.size();
		currentCandidates.push_back(candidates[i]);
	}

	attemptStartMerge(bmData, currentCandidates);

	return Void();
}

// Uses single-pass algorithm to identify mergeable sections of granules.
// To ensure each granule waits to see whether all of its neighbors are merge-eligible before merging it, a newly
// merge-eligible granule will be ignored on the first pass
ACTOR Future<Void> granuleMergeChecker(Reference<BlobManagerData> bmData) {
	// wait for BM data to have loaded hard boundaries before starting
	wait(bmData->loadedClientRanges.getFuture());
	// initial sleep
	wait(delayJittered(SERVER_KNOBS->BG_MERGE_CANDIDATE_DELAY_SECONDS));
	// TODO could optimize to not check if there are no new merge-eligible granules and none in merge pending state
	loop {

		double sleepTime = SERVER_KNOBS->BG_MERGE_CANDIDATE_DELAY_SECONDS;
		// Check more frequently if speedUpSimulation is set. This may
		if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
			sleepTime = std::min(5.0, sleepTime);
		}
		// start delay at the start of the loop, to account for time spend in calculation
		state Future<Void> intervalDelay = delayJittered(sleepTime);

		// go over granule states, and start a findMergeableGranules for each sub-range of mergeable granules
		// FIXME: avoid SlowTask by breaking this up periodically

		// Break it up into parallel chunks. This makes it possible to process large ranges, but does mean the merges
		// can be slightly suboptimal at boundaries. Use relatively large chunks to minimize the impact of this.
		int maxRangeSize = SERVER_KNOBS->BG_MAX_MERGE_FANIN * 10;

		state std::vector<Future<Void>> mergeChecks;
		auto allRanges = bmData->mergeCandidates.ranges();
		std::vector<std::tuple<UID, KeyRange, Version>> currentCandidates;
		auto& mergeBoundaries = bmData->mergeBoundaries;
		for (auto& it : allRanges) {
			// Conditions:
			//   1. Next range is not eligible.
			//   2. Hit the maximum in a merge evaluation window.
			//   3. Hit a hard merge boundary meaning we should not merge across them.
			if (!it->cvalue().mergeEligible() || currentCandidates.size() == maxRangeSize ||
			    bmData->mergeHardBoundaries.count(it->range().begin)) {
				if (currentCandidates.size() >= 2) {
					mergeChecks.push_back(attemptMerges(bmData, currentCandidates));
				}
				currentCandidates.clear();
			}

			// Soft boundaries.
			// We scan all ranges including non-eligible ranges which trigger non-consecutive merge flushes.
			if (!currentCandidates.empty()) {
				KeyRangeRef lastRange = std::get<1>(currentCandidates.back());
				KeyRangeRef curRange = it->range();
				ASSERT(lastRange.end == curRange.begin);
				// Conditions:
				//   1. Start a new soft merge range.
				//   2. End a soft merge range.
				if ((!mergeBoundaries.count(curRange.begin) && mergeBoundaries.count(curRange.end)) ||
				    (mergeBoundaries.count(lastRange.begin) && !mergeBoundaries.count(lastRange.end))) {
					if (currentCandidates.size() >= 2) {
						mergeChecks.push_back(attemptMerges(bmData, currentCandidates));
					}
					currentCandidates.clear();
				}
			}

			if (it->cvalue().mergeEligible()) {
				currentCandidates.push_back(std::tuple(it->cvalue().granuleID, it->range(), it->cvalue().startVersion));
			} else if (it->cvalue().canMerge()) {
				// set flag so this can get merged on the next pass
				it->value().seen = true;
			}
		}
		if (currentCandidates.size() >= 2) {
			mergeChecks.push_back(attemptMerges(bmData, currentCandidates));
		}

		CODE_PROBE(mergeChecks.size() > 1, "parallel merge checks");
		wait(waitForAll(mergeChecks));
		// if the calculation took longer than the desired interval, still wait a bit
		wait(intervalDelay && delay(5.0));
	}
}

ACTOR Future<Void> deregisterBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface interf) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			wait(checkManagerLock(tr, bmData));
			Key blobWorkerListKey = blobWorkerListKeyFor(interf.id());
			// FIXME: should be able to remove this conflict range
			tr->addReadConflictRange(singleKeyRange(blobWorkerListKey));
			tr->clear(blobWorkerListKey);

			wait(tr->commit());

			if (BM_DEBUG) {
				fmt::print("Deregistered blob worker {0}\n", interf.id().toString());
			}
			return Void();
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("Deregistering blob worker {0} got error {1}\n", interf.id().toString(), e.name());
			}
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> haltBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf) {
	loop {
		try {
			wait(bwInterf.haltBlobWorker.getReply(HaltBlobWorkerRequest(bmData->epoch, bmData->id)));
			break;
		} catch (Error& e) {
			// throw other errors instead of returning?
			if (e.code() == error_code_operation_cancelled) {
				throw;
			}
			if (e.code() != error_code_blob_manager_replaced) {
				break;
			}
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.send(Void());
			}
		}
	}

	return Void();
}

ACTOR Future<Void> killBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf, bool registered) {
	state UID bwId = bwInterf.id();

	// Remove blob worker from stats map so that when we try to find a worker to takeover the range,
	// the one we just killed isn't considered.
	// Remove it from workersById also since otherwise that worker addr will remain excluded
	// when we try to recruit new blob workers.

	TraceEvent("KillBlobWorker", bmData->id).detail("Epoch", bmData->epoch).detail("WorkerId", bwId);

	if (registered) {
		bmData->deadWorkers.insert(bwId);
		bmData->workerStats.erase(bwId);
		bmData->workersById.erase(bwId);
		bmData->workerAddresses.erase(bwInterf.stableAddress());
	}

	// Remove blob worker from persisted list of blob workers
	Future<Void> deregister = deregisterBlobWorker(bmData, bwInterf);

	// for every range owned by this blob worker, we want to
	// - send a revoke request for that range
	// - add the range back to the stream of ranges to be assigned
	if (BM_DEBUG) {
		fmt::print("Taking back ranges from BW {0}\n", bwId.toString());
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
		handleRangeAssign(bmData, raRevoke);

		// Add range back into the stream of ranges to be assigned
		RangeAssignment raAssign;
		raAssign.isAssign = true;
		raAssign.worker = Optional<UID>();
		raAssign.keyRange = it;
		raAssign.assign = RangeAssignmentData(); // not a continue
		handleRangeAssign(bmData, raAssign);
	}

	// Send halt to blob worker, with no expectation of hearing back
	if (BM_DEBUG) {
		fmt::print("Sending halt to BW {}\n", bwId.toString());
	}
	bmData->addActor.send(haltBlobWorker(bmData, bwInterf));

	// wait for blob worker to be removed from DB and in-memory mapping to have reassigned all shards from this worker
	// before removing it from deadWorkers, to avoid a race with checkBlobWorkerList
	wait(deregister);

	// restart recruiting to replace the dead blob worker
	bmData->restartRecruiting.trigger();

	if (registered) {
		bmData->deadWorkers.erase(bwInterf.id());
	}

	return Void();
}

ACTOR Future<Void> monitorBlobWorkerStatus(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf) {
	// outer loop handles reconstructing stream if it got a retryable error
	// do backoff, we can get a lot of retries in a row

	// wait for blob manager to be done recovering, so it has initial granule mapping and worker data
	wait(bmData->doneRecovering.getFuture());
	wait(delay(0));

	state double backoff = SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_MIN;
	loop {
		try {
			state ReplyPromiseStream<GranuleStatusReply> statusStream =
			    bwInterf.granuleStatusStreamRequest.getReplyStream(GranuleStatusStreamRequest(bmData->epoch));
			// read from stream until worker fails (should never get explicit end_of_stream)
			loop {
				GranuleStatusReply rep = waitNext(statusStream.getFuture());

				if (BM_DEBUG) {
					fmt::print("BM {0} got status of [{1} - {2}) @ ({3}, {4}) from BW {5}: {6} {7}\n",
					           bmData->epoch,
					           rep.granuleRange.begin.printable(),
					           rep.granuleRange.end.printable(),
					           rep.continueEpoch,
					           rep.continueSeqno,
					           bwInterf.id().toString(),
					           rep.doSplit ? "split" : (rep.mergeCandidate ? "merge" : ""),
					           rep.mergeCandidate
					               ? ""
					               : (rep.writeHotSplit ? "hot" : (rep.initialSplitTooBig ? "toobig" : "normal")));
				}

				ASSERT(rep.doSplit || rep.mergeCandidate);

				// if we get a reply from the stream, reset backoff
				backoff = SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_MIN;
				if (rep.continueEpoch > bmData->epoch) {
					if (BM_DEBUG) {
						fmt::print("BM {0} heard from BW {1} that there is a new manager with higher epoch\n",
						           bmData->epoch,
						           bwInterf.id().toString());
					}
					if (bmData->iAmReplaced.canBeSet()) {
						bmData->iAmReplaced.send(Void());
					}
				}

				BoundaryEvaluation newEval(rep.continueEpoch,
				                           rep.continueSeqno,
				                           rep.doSplit ? BoundaryEvalType::SPLIT : BoundaryEvalType::MERGE,
				                           rep.originalEpoch,
				                           rep.originalSeqno);

				bool ignore = false;
				Optional<std::pair<KeyRange, BoundaryEvaluation>> existingInProgress;
				auto lastBoundaryEvals = bmData->boundaryEvaluations.intersectingRanges(rep.granuleRange);
				for (auto& lastBoundaryEval : lastBoundaryEvals) {
					if (ignore) {
						break;
					}
					if (rep.granuleRange.begin == lastBoundaryEval.begin() &&
					    rep.granuleRange.end == lastBoundaryEval.end() && newEval == lastBoundaryEval.cvalue()) {
						if (BM_DEBUG) {
							fmt::print("BM {0} received repeat status for the same granule [{1} - {2}) {3}, "
							           "ignoring.\n",
							           bmData->epoch,
							           rep.granuleRange.begin.printable(),
							           rep.granuleRange.end.printable(),
							           newEval.toString());
						}
						ignore = true;
					} else if (newEval < lastBoundaryEval.cvalue()) {
						CODE_PROBE(true, "BM got out-of-date split request");
						if (BM_DEBUG) {
							fmt::print("BM {0} ignoring status from BW {1} for granule [{2} - {3}) {4} since it "
							           "already processed [{5} - {6}) {7}.\n",
							           bmData->epoch,
							           bwInterf.id().toString().substr(0, 5),
							           rep.granuleRange.begin.printable(),
							           rep.granuleRange.end.printable(),
							           newEval.toString(),
							           lastBoundaryEval.begin().printable(),
							           lastBoundaryEval.end().printable(),
							           lastBoundaryEval.cvalue().toString());
						}

						// only revoke if original epoch + seqno is older, different assignment
						if (newEval.isOlderThanOriginal(lastBoundaryEval.cvalue())) {
							// revoke range from out-of-date worker, but bypass rangeAssigner and hack (epoch, seqno) to
							// be (requesting epoch, requesting seqno + 1) to ensure no race with then reassigning the
							// range to the worker at a later version

							if (BM_DEBUG) {
								fmt::print("BM {0} revoking from BW {1} granule [{2} - {3}) {4} with original ({5}, "
								           "{6}) since it already processed original ({7}, {8}).\n",
								           bmData->epoch,
								           bwInterf.id().toString().substr(0, 5),
								           rep.granuleRange.begin.printable(),
								           rep.granuleRange.end.printable(),
								           newEval.toString(),
								           newEval.originalEpoch,
								           newEval.originalSeqno,
								           lastBoundaryEval.cvalue().originalEpoch,
								           lastBoundaryEval.cvalue().originalSeqno);
							}
							RangeAssignment revokeOld;
							revokeOld.isAssign = false;
							revokeOld.worker = bwInterf.id();
							revokeOld.keyRange = rep.granuleRange;
							revokeOld.revoke = RangeRevokeData(false);

							bmData->addActor.send(doRangeAssignment(
							    bmData, revokeOld, bwInterf.id(), rep.continueEpoch, rep.continueSeqno + 1));
						}

						ignore = true;
					} else if (lastBoundaryEval.cvalue().inProgress.isValid() &&
					           !lastBoundaryEval.cvalue().inProgress.isReady()) {
						existingInProgress = std::pair(lastBoundaryEval.range(), lastBoundaryEval.value());
					}
				}
				if (rep.doSplit && !ignore) {
					ASSERT(!rep.mergeCandidate);

					bool clearMergeCandidate = !existingInProgress.present() ||
					                           existingInProgress.get().second.type != BoundaryEvalType::MERGE;

					// Check for split/merge race
					Version inProgressMergeVersion = bmData->activeMergeVersion(rep.granuleRange);

					if (BM_DEBUG) {
						fmt::print("BM {0} splt eval [{1} - {2}). existing={3}, inProgressMergeVersion={4}, "
						           "blockedVersion={5}\n",
						           bmData->epoch,
						           rep.granuleRange.begin.printable().c_str(),
						           rep.granuleRange.end.printable().c_str(),
						           existingInProgress.present() ? "T" : "F",
						           inProgressMergeVersion,
						           rep.blockedVersion);
					}

					// If the in progress one is a merge, and the blockedVersion < the mergeVersion, this granule
					// needs to continue to flush up to the merge version. If the merge intent is still not
					// persisted, the version will be invalidVersion, so this should only happen after the merge
					// intent is persisted and the merge version is fixed. This can happen if a merge candidate
					// suddenly gets a burst of writes after a decision to merge is made
					if (inProgressMergeVersion != invalidVersion) {
						if (rep.blockedVersion < inProgressMergeVersion) {
							CODE_PROBE(true, "merge blocking re-snapshot");
							if (BM_DEBUG) {
								fmt::print("BM {0} MERGE @ {1} blocking re-snapshot [{2} - {3}) @ {4}, "
								           "continuing snapshot\n",
								           bmData->epoch,
								           inProgressMergeVersion,
								           rep.granuleRange.begin.printable(),
								           rep.granuleRange.end.printable(),
								           rep.blockedVersion);
							}
							RangeAssignment raContinue;
							raContinue.isAssign = true;
							raContinue.worker = bwInterf.id();
							raContinue.keyRange = rep.granuleRange;
							raContinue.assign =
							    RangeAssignmentData(AssignRequestType::Continue); // continue assignment and re-snapshot
							handleRangeAssign(bmData, raContinue);
						}
						clearMergeCandidate = false;
						ignore = true;
					} else if (existingInProgress.present()) {
						// For example, one worker asked BM to split, then died, granule was moved, new worker asks
						// to split on recovery. We need to ensure that they are semantically the same split. We
						// will just rely on the in-progress split to finish
						if (BM_DEBUG) {
							fmt::print("BM {0} got request for [{1} - {2}) {3}, but already in "
							           "progress from [{4} - {5}) {6}\n",
							           bmData->epoch,
							           rep.granuleRange.begin.printable(),
							           rep.granuleRange.end.printable(),
							           newEval.toString(),
							           existingInProgress.get().first.begin.printable(),
							           existingInProgress.get().first.end.printable(),
							           existingInProgress.get().second.toString());
						}

						// ignore the request, they will retry
						ignore = true;
					}
					if (!ignore) {
						if (BM_DEBUG) {
							fmt::print("BM {0} evaluating [{1} - {2}) {3}\n",
							           bmData->epoch,
							           rep.granuleRange.begin.printable().c_str(),
							           rep.granuleRange.end.printable().c_str(),
							           newEval.toString());
						}
						if (rep.initialSplitTooBig) {
							ASSERT(rep.proposedSplitKey.present());
							newEval.inProgress = reevaluateInitialSplit(bmData,
							                                            bwInterf.id(),
							                                            rep.granuleRange,
							                                            rep.granuleID,
							                                            rep.originalEpoch,
							                                            rep.originalSeqno,
							                                            rep.proposedSplitKey.get());
						} else {
							newEval.inProgress = maybeSplitRange(bmData,
							                                     bwInterf.id(),
							                                     rep.granuleRange,
							                                     rep.granuleID,
							                                     rep.startVersion,
							                                     rep.writeHotSplit,
							                                     rep.originalEpoch,
							                                     rep.originalSeqno);
						}
						bmData->boundaryEvaluations.insert(rep.granuleRange, newEval);
					}

					// clear merge candidates for range, if not already merging
					if (clearMergeCandidate) {
						bmData->clearMergeCandidate(rep.granuleRange);
					}
				}
				if (rep.mergeCandidate && !ignore) {
					// mark granule as merge candidate
					ASSERT(!rep.doSplit);
					CODE_PROBE(true, "Granule merge candidate");
					if (BM_DEBUG) {
						fmt::print("Manager {0} merge candidate granule [{1} - {2}) {3}\n",
						           bmData->epoch,
						           rep.granuleRange.begin.printable().c_str(),
						           rep.granuleRange.end.printable().c_str(),
						           newEval.toString());
					}
					bmData->boundaryEvaluations.insert(rep.granuleRange, newEval);
					bmData->setMergeCandidate(rep.granuleRange, rep.granuleID, rep.startVersion);
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled) {
				throw e;
			}

			// on known network errors or stream close errors, throw
			if (e.code() == error_code_broken_promise) {
				throw e;
			}

			// if manager is replaced, die
			if (e.code() == error_code_blob_manager_replaced) {
				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.send(Void());
				}
				return Void();
			}

			// if we got an error constructing or reading from stream that is retryable, wait and retry.
			// Sometimes we get connection_failed without the failure monitor tripping. One example is simulation's
			// rollRandomClose. In this case, just reconstruct the stream. If it was a transient failure, it works, and
			// if it is permanent, the failure monitor will eventually trip.
			ASSERT(e.code() != error_code_end_of_stream);
			if (e.code() == error_code_request_maybe_delivered || e.code() == error_code_connection_failed) {
				CODE_PROBE(true, "BM retrying BW monitoring");
				wait(delay(backoff));
				backoff = std::min(backoff * SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_EXPONENT,
				                   SERVER_KNOBS->BLOB_MANAGER_STATUS_EXP_BACKOFF_MAX);
				continue;
			} else {
				TraceEvent(SevError, "BlobManagerUnexpectedErrorStatusMonitoring", bmData->id)
				    .error(e)
				    .detail("Epoch", bmData->epoch);
				ASSERT_WE_THINK(false);
				// if not simulation, kill the BM
				if (bmData->iAmReplaced.canBeSet()) {
					bmData->iAmReplaced.sendError(e);
				}
				throw e;
			}
		}
	}
}

ACTOR Future<Void> monitorBlobWorker(Reference<BlobManagerData> bmData, BlobWorkerInterface bwInterf) {
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
				// should only return when manager got replaced
				ASSERT(!bmData->iAmReplaced.canBeSet());
			}
		}
	} catch (Error& e) {
		// will blob worker get cleaned up in this case?
		if (e.code() == error_code_operation_cancelled) {
			throw e;
		}

		if (BM_DEBUG) {
			fmt::print(
			    "BM {0} got monitoring error {1} from BW {2}\n", bmData->epoch, e.name(), bwInterf.id().toString());
		}

		// Expected errors here are: [broken_promise]
		if (e.code() != error_code_broken_promise) {
			if (BM_DEBUG) {
				fmt::print("BM got unexpected error {0} monitoring BW {1}\n", e.name(), bwInterf.id().toString());
			}
			TraceEvent(SevError, "BlobManagerUnexpectedErrorMonitorBW", bmData->id)
			    .error(e)
			    .detail("Epoch", bmData->epoch);
			ASSERT_WE_THINK(false);
			// if not simulation, kill the BM
			if (bmData->iAmReplaced.canBeSet()) {
				bmData->iAmReplaced.sendError(e);
			}
			throw e;
		}
	}

	// kill the blob worker
	wait(killBlobWorker(bmData, bwInterf, true));

	if (BM_DEBUG) {
		fmt::print("No longer monitoring BW {0}\n", bwInterf.id().toString());
	}
	return Void();
}

ACTOR Future<Void> checkBlobWorkerList(Reference<BlobManagerData> bmData, Promise<Void> workerListReady) {

	try {
		loop {
			// Get list of last known blob workers
			// note: the list will include every blob worker that the old manager knew about,
			// but it might also contain blob workers that died while the new manager was being recruited
			std::vector<BlobWorkerInterface> blobWorkers = wait(getBlobWorkers(bmData->db));
			// add all blob workers to this new blob manager's records and start monitoring it
			bool foundAnyNew = false;
			for (auto& worker : blobWorkers) {
				if (!bmData->deadWorkers.count(worker.id())) {
					if (!bmData->workerAddresses.count(worker.stableAddress()) &&
					    worker.locality.dcId() == bmData->dcId) {
						bmData->workerAddresses.insert(worker.stableAddress());
						bmData->workersById[worker.id()] = worker;
						bmData->workerStats[worker.id()] = BlobWorkerInfo();
						bmData->addActor.send(monitorBlobWorker(bmData, worker));
						foundAnyNew = true;
					} else if (!bmData->workersById.count(worker.id())) {
						bmData->addActor.send(killBlobWorker(bmData, worker, false));
					}
				}
			}
			if (workerListReady.canBeSet()) {
				workerListReady.send(Void());
			}
			// if any assigns are stuck on workers, and we have workers, wake them
			if (foundAnyNew || !bmData->workersById.empty()) {
				Promise<Void> hold = bmData->foundBlobWorkers;
				bmData->foundBlobWorkers = Promise<Void>();
				hold.send(Void());
			}
			wait(delay(SERVER_KNOBS->BLOB_WORKERLIST_FETCH_INTERVAL));
		}
	} catch (Error& e) {
		if (BM_DEBUG) {
			fmt::print("BM {0} got error {1} reading blob worker list!!\n", bmData->epoch, e.name());
		}
		throw e;
	}
}
// Shared code for handling KeyRangeMap<tuple(UID, epoch, seqno)> that is used several places in blob manager recovery
// when there can be conflicting sources of what assignments exist or which workers owns a granule.
// Resolves these conflicts by comparing the epoch + seqno for the range
// Special epoch/seqnos:
//   (0,0): range is not mapped
static void addAssignment(KeyRangeMap<std::tuple<UID, int64_t, int64_t>>& map,
                          const KeyRangeRef& newRange,
                          UID newId,
                          int64_t newEpoch,
                          int64_t newSeqno,
                          std::vector<std::pair<UID, KeyRange>>& outOfDate) {
	std::vector<std::pair<KeyRange, std::tuple<UID, int64_t, int64_t>>> newer;
	auto intersecting = map.intersectingRanges(newRange);
	bool allExistingNewer = true;
	bool anyConflicts = false;
	for (auto& old : intersecting) {
		UID oldWorker = std::get<0>(old.value());
		int64_t oldEpoch = std::get<1>(old.value());
		int64_t oldSeqno = std::get<2>(old.value());
		if (oldEpoch > newEpoch || (oldEpoch == newEpoch && oldSeqno > newSeqno)) {
			newer.push_back(std::pair(old.range(), std::tuple(oldWorker, oldEpoch, oldSeqno)));
			if (old.range() != newRange) {
				CODE_PROBE(true, "BM Recovery: BWs disagree on range boundaries");
				anyConflicts = true;
			}
		} else {
			allExistingNewer = false;
			if (newId != UID() && newEpoch != std::numeric_limits<int64_t>::max()) {
				// different workers can't have same epoch and seqno for granule assignment
				ASSERT(oldEpoch != newEpoch || oldSeqno != newSeqno);
			}
			if (newEpoch == std::numeric_limits<int64_t>::max() && (oldWorker != newId || old.range() != newRange)) {
				CODE_PROBE(true, "BM Recovery: DB disagrees with workers");
				// new one is from DB (source of truth on boundaries) and existing mapping disagrees on boundary or
				// assignment, do explicit revoke and re-assign to converge
				anyConflicts = true;
				// if ranges don't match, need to explicitly reassign all parts of old range, as it could be from a
				// yet-unassigned split
				if (old.range() != newRange) {
					std::get<0>(old.value()) = UID();
				}
				if (oldWorker != UID() &&
				    (outOfDate.empty() || outOfDate.back() != std::pair(oldWorker, KeyRange(old.range())))) {

					outOfDate.push_back(std::pair(oldWorker, old.range()));
				}
			} else if (oldWorker != UID() && oldWorker != newId &&
			           (oldEpoch < newEpoch || (oldEpoch == newEpoch && oldSeqno < newSeqno))) {
				// 2 blob workers reported conflicting mappings, add old one to out of date (if not already added by a
				// previous intersecting range in the split case)
				// if ranges don't match, need to explicitly reassign all parts of old range, as it could be from a
				// partially-assigned split
				if (old.range() != newRange) {
					std::get<0>(old.value()) = UID();
				}
				if (outOfDate.empty() || outOfDate.back() != std::pair(oldWorker, KeyRange(old.range()))) {
					CODE_PROBE(true, "BM Recovery: Two workers claim ownership of same granule");
					outOfDate.push_back(std::pair(oldWorker, old.range()));
				}
			}
		}
	}

	if (!allExistingNewer) {
		// if this range supercedes an old range insert it over that
		map.insert(newRange, std::tuple(anyConflicts ? UID() : newId, newEpoch, newSeqno));

		// then, if there were any ranges superceded by this one, insert them over this one
		if (newer.size()) {
			if (newId != UID()) {
				outOfDate.push_back(std::pair(newId, newRange));
			}
			for (auto& it : newer) {
				map.insert(it.first, it.second);
			}
		}
	} else {
		if (newId != UID()) {
			outOfDate.push_back(std::pair(newId, newRange));
		}
	}
}

// essentially just error handling for resumed merge, since doMerge does it for new merge
ACTOR Future<Void> resumeMerge(Future<Void> finishMergeFuture, KeyRange mergeRange) {
	try {
		wait(finishMergeFuture);
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_operation_cancelled || e.code() == error_code_blob_manager_replaced) {
			throw;
		}
		TraceEvent(SevError, "UnexpectedErrorResumeGranuleMerge").error(e).detail("Range", mergeRange);
		throw e;
	}
}

ACTOR Future<Void> loadForcePurgedRanges(Reference<BlobManagerData> bmData) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state Key beginKey = blobGranuleForcePurgedKeys.begin;
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(2, 10) : 10000;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			// using the krm functions can produce incorrect behavior here as it does weird stuff with beginKey
			KeyRange nextRange(KeyRangeRef(beginKey, blobGranuleForcePurgedKeys.end));
			state GetRangeLimits limits(rowLimit, GetRangeLimits::BYTE_LIMIT_UNLIMITED);
			limits.minRows = 2;
			RangeResult results = wait(tr->getRange(nextRange, limits));

			// Add the mappings to our in memory key range map
			for (int rangeIdx = 0; rangeIdx < results.size() - 1; rangeIdx++) {
				if (results[rangeIdx].value == "1"_sr) {
					Key rangeStartKey = results[rangeIdx].key.removePrefix(blobGranuleForcePurgedKeys.begin);
					Key rangeEndKey = results[rangeIdx + 1].key.removePrefix(blobGranuleForcePurgedKeys.begin);
					// note: if the old owner is dead, we handle this in rangeAssigner
					bmData->forcePurgingRanges.insert(KeyRangeRef(rangeStartKey, rangeEndKey), true);
				}
			}

			if (!results.more || results.size() <= 1) {
				break;
			}

			// re-read last key to get range that starts there
			beginKey = results.back().key;
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print(
				    "BM {0} got error reading force purge ranges during recovery: {1}\n", bmData->epoch, e.name());
			}
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> resumeActiveMerges(Reference<BlobManagerData> bmData, Future<Void> loadForcePurgedRanges) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	wait(loadForcePurgedRanges); // load these first, to make sure to avoid resuming any merges that are being force
	                             // purged

	// FIXME: use range stream instead
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(1, 10) : 10000;
	state KeyRange currentRange = blobGranuleMergeKeys;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			RangeResult result = wait(tr->getRange(currentRange, rowLimit));
			state bool anyMore = result.more;
			state std::vector<Future<Void>> cleanupForcePurged;
			for (auto& it : result) {
				CODE_PROBE(true, "Blob Manager Recovery found merging granule");
				UID mergeGranuleID = decodeBlobGranuleMergeKey(it.key);
				KeyRange mergeRange;
				std::vector<UID> parentGranuleIDs;
				std::vector<Key> parentGranuleRanges;
				std::vector<Version> parentGranuleStartVersions;
				Version mergeVersion;
				std::tie(mergeRange, mergeVersion, parentGranuleIDs, parentGranuleRanges, parentGranuleStartVersions) =
				    decodeBlobGranuleMergeValue(it.value);

				if (BM_DEBUG) {
					fmt::print("BM {0} found merge in progress: [{1} - {2}) @ {3}\n",
					           bmData->epoch,
					           mergeRange.begin.printable(),
					           mergeRange.end.printable(),
					           mergeVersion);
				}

				if (bmData->isForcePurging(mergeRange)) {
					if (BM_DEBUG) {
						fmt::print(
						    "BM {0} cleaning up merge [{1} - {2}): {3} @ {4} in progress b/c it was force purged\n",
						    bmData->epoch,
						    mergeRange.begin.printable(),
						    mergeRange.end.printable(),
						    mergeGranuleID.toString().substr(0, 6),
						    mergeVersion);
					}
					cleanupForcePurged.push_back(updateChangeFeed(&tr->getTransaction(),
					                                              granuleIDToCFKey(mergeGranuleID),
					                                              ChangeFeedStatus::CHANGE_FEED_DESTROY,
					                                              mergeRange));
					continue;
				}

				// want to mark in progress granule ranges as merging, BEFORE recovery is complete and workers can
				// report updated status. Start with early (epoch, seqno) to guarantee lower than later status
				BoundaryEvaluation eval(1, 0, BoundaryEvalType::MERGE, 1, 0);
				ASSERT(!bmData->isMergeActive(mergeRange));
				bmData->addActor.send(resumeMerge(finishMergeGranules(bmData,
				                                                      mergeGranuleID,
				                                                      mergeRange,
				                                                      mergeVersion,
				                                                      parentGranuleIDs,
				                                                      parentGranuleRanges,
				                                                      parentGranuleStartVersions),
				                                  mergeRange));
				bmData->boundaryEvaluations.insert(mergeRange, eval);
				bmData->activeGranuleMerges.insert(mergeRange, mergeVersion);
				bmData->setMergeCandidate(mergeRange, MergeCandidateMerging);
			}

			if (anyMore) {
				currentRange = KeyRangeRef(keyAfter(result.back().key), currentRange.end);
			}

			if (!cleanupForcePurged.empty()) {
				wait(waitForAll(cleanupForcePurged));
				wait(tr->commit());
				tr->reset();
			}

			if (!anyMore) {
				return Void();
			}
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> loadBlobGranuleMergeBoundaries(Reference<BlobManagerData> bmData) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(2, 10) : 10000;
	state Key beginKey = blobGranuleMergeBoundaryKeys.begin;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			KeyRange nextRange(KeyRangeRef(beginKey, blobGranuleMergeBoundaryKeys.end));
			// using the krm functions can produce incorrect behavior here as it does weird stuff with beginKey
			state GetRangeLimits limits(rowLimit, GetRangeLimits::BYTE_LIMIT_UNLIMITED);
			RangeResult results = wait(tr->getRange(nextRange, limits));

			// Add the mappings to our in memory key range map
			for (int i = 0; i < results.size(); i++) {
				bmData->mergeBoundaries[results[i].key.removePrefix(blobGranuleMergeBoundaryKeys.begin)] =
				    decodeBlobGranuleMergeBoundaryValue(results[i].value);
			}

			if (!results.more) {
				break;
			}
			beginKey = keyAfter(results.back().key);
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("BM {0} got error reading granule merge boundaries during recovery: {1}\n",
				           bmData->epoch,
				           e.name());
			}
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> recoverBlobManager(Reference<BlobManagerData> bmData) {
	state double recoveryStartTime = now();
	state Promise<Void> workerListReady;
	bmData->addActor.send(checkBlobWorkerList(bmData, workerListReady));
	wait(workerListReady.getFuture());

	state std::vector<BlobWorkerInterface> startingWorkers;
	for (auto& it : bmData->workersById) {
		startingWorkers.push_back(it.second);
	}

	// Once we acknowledge the existing blob workers, we can go ahead and recruit new ones
	bmData->startRecruiting.trigger();

	bmData->initBStore();
	if (isFullRestoreMode())
		wait(loadManifest(bmData->db, bmData->bstore));

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

	// set up force purge keys if not done already
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult existingForcePurgeKeys = wait(tr->getRange(blobGranuleForcePurgedKeys, 1));
			if (!existingForcePurgeKeys.empty()) {
				break;
			}
			wait(checkManagerLock(tr, bmData));
			wait(krmSetRange(tr, blobGranuleForcePurgedKeys.begin, normalKeys, "0"_sr));
			wait(tr->commit());
			tr->reset();
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// skip the rest of the algorithm for the first blob manager
	if (bmData->epoch == 1 && !isFullRestoreMode()) {
		bmData->doneRecovering.send(Void());
		return Void();
	}

	state Future<Void> forcePurgedRanges = loadForcePurgedRanges(bmData);
	state Future<Void> resumeMergesFuture = resumeActiveMerges(bmData, forcePurgedRanges);

	CODE_PROBE(true, "BM doing recovery");

	wait(delay(0));

	// At this point, bmData->workersById is a list of all alive blob workers, but could also include some dead BWs.
	// The algorithm below works as follows:
	//
	// 1. We get the existing granule mappings. We do this by asking all active blob workers for their current granule
	//    assignments. This guarantees a consistent snapshot of the state of that worker's assignments: Any request it
	//    recieved and processed from the old manager before the granule assignment request will be included in the
	//    assignments, and any request it recieves from the old manager afterwards will be rejected with
	//    blob_manager_replaced. We then read from the database as the source of truth for the assignment. We will
	//    reconcile the set of ongoing splits to this mapping, and any ranges that are not already assigned to existing
	//    blob workers will be reassigned.
	//
	// 2. For every range in our granuleAssignments, we send an assign request to the stream of requests,
	//    ultimately giving every range back to some worker (trying to mimic the state of the old BM).
	//    If the worker already had the range, this is a no-op. If the worker didn't have it, it will
	//    begin persisting it. The worker that had the same range before will now be at a lower seqno.

	state KeyRangeMap<std::tuple<UID, int64_t, int64_t>> workerAssignments;
	workerAssignments.insert(normalKeys, std::tuple(UID(), 0, 0));

	// FIXME: use range stream instead
	state int rowLimit = BUGGIFY ? deterministicRandom()->randomInt(2, 10) : 10000;

	if (BM_DEBUG) {
		fmt::print("BM {0} recovering:\n", bmData->epoch);
	}

	// Step 1. Get the latest known mapping of granules to blob workers (i.e. assignments)
	// This must happen causally AFTER reading the split boundaries, since the blob workers can clear the split
	// boundaries for a granule as part of persisting their assignment.

	// First, ask existing workers for their mapping
	if (BM_DEBUG) {
		fmt::print("BM {0} requesting assignments from {1} workers:\n", bmData->epoch, startingWorkers.size());
	}
	state std::vector<Future<Optional<GetGranuleAssignmentsReply>>> aliveAssignments;
	aliveAssignments.reserve(startingWorkers.size());
	for (auto& it : startingWorkers) {
		GetGranuleAssignmentsRequest req;
		req.managerEpoch = bmData->epoch;
		aliveAssignments.push_back(timeout(brokenPromiseToNever(it.granuleAssignmentsRequest.getReply(req)),
		                                   SERVER_KNOBS->BLOB_WORKER_TIMEOUT));
	}

	state std::vector<std::pair<UID, KeyRange>> outOfDateAssignments;
	state int successful = 0;
	state int assignIdx = 0;

	for (; assignIdx < aliveAssignments.size(); assignIdx++) {
		Optional<GetGranuleAssignmentsReply> reply = wait(aliveAssignments[assignIdx]);
		UID workerId = startingWorkers[assignIdx].id();

		if (reply.present()) {
			if (BM_DEBUG) {
				fmt::print("  Worker {}: ({})\n", workerId.toString().substr(0, 5), reply.get().assignments.size());
			}
			successful++;
			for (auto& assignment : reply.get().assignments) {
				if (BM_DEBUG) {
					fmt::print("    [{0} - {1}): ({2}, {3})\n",
					           assignment.range.begin.printable(),
					           assignment.range.end.printable(),
					           assignment.epochAssigned,
					           assignment.seqnoAssigned);
				}
				bmData->knownBlobRanges.insert(assignment.range, true);
				addAssignment(workerAssignments,
				              assignment.range,
				              workerId,
				              assignment.epochAssigned,
				              assignment.seqnoAssigned,
				              outOfDateAssignments);
			}
			if (bmData->workerStats.count(workerId)) {
				bmData->workerStats[workerId].numGranulesAssigned = reply.get().assignments.size();
			}
		} else {
			CODE_PROBE(true, "BM Recovery: BW didn't respond to assignments request");
			// SOMEDAY: mark as failed and kill it
			if (BM_DEBUG) {
				fmt::print("  Worker {}: failed\n", workerId.toString().substr(0, 5));
			}
		}
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} got assignments from {1}/{2} workers:\n", bmData->epoch, successful, startingWorkers.size());
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} found old assignments:\n", bmData->epoch);
	}

	// DB is the source of truth, so read from here, and resolve any conflicts with current worker mapping
	// We don't have a consistent snapshot of the mapping ACROSS blob workers, so we need the DB to reconcile any
	// differences (eg blob manager revoked from worker A, assigned to B, the revoke from A was processed but the assign
	// to B wasn't, meaning in the snapshot nobody owns the granule). This also handles races with a BM persisting a
	// boundary change, then dying before notifying the workers
	state Key beginKey = blobGranuleMappingKeys.begin;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			KeyRange nextRange(KeyRangeRef(beginKey, blobGranuleMappingKeys.end));
			// using the krm functions can produce incorrect behavior here as it does weird stuff with beginKey
			state GetRangeLimits limits(rowLimit, GetRangeLimits::BYTE_LIMIT_UNLIMITED);
			limits.minRows = 2;
			RangeResult results = wait(tr->getRange(nextRange, limits));

			// Add the mappings to our in memory key range map
			for (int rangeIdx = 0; rangeIdx < results.size() - 1; rangeIdx++) {
				Key granuleStartKey = results[rangeIdx].key.removePrefix(blobGranuleMappingKeys.begin);
				Key granuleEndKey = results[rangeIdx + 1].key.removePrefix(blobGranuleMappingKeys.begin);
				if (results[rangeIdx].value.size()) {
					// note: if the old owner is dead, we handle this in rangeAssigner
					UID existingOwner = decodeBlobGranuleMappingValue(results[rangeIdx].value);
					// use (max int64_t, 0) to be higher than anything that existing workers have
					addAssignment(workerAssignments,
					              KeyRangeRef(granuleStartKey, granuleEndKey),
					              existingOwner,
					              std::numeric_limits<int64_t>::max(),
					              0,
					              outOfDateAssignments);

					bmData->knownBlobRanges.insert(KeyRangeRef(granuleStartKey, granuleEndKey), true);
					if (BM_DEBUG) {
						fmt::print("  [{0} - {1})={2}\n",
						           granuleStartKey.printable(),
						           granuleEndKey.printable(),
						           existingOwner.toString().substr(0, 5));
					}
				} else {
					if (BM_DEBUG) {
						fmt::print("  [{0} - {1})\n", granuleStartKey.printable(), granuleEndKey.printable());
					}
				}
			}

			if (!results.more || results.size() <= 1) {
				break;
			}

			// re-read last key to get range that starts there
			beginKey = results.back().key;
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("BM {0} got error reading granule mapping during recovery: {1}\n", bmData->epoch, e.name());
			}
			wait(tr->onError(e));
		}
	}

	wait(forcePurgedRanges);
	wait(resumeMergesFuture);

	// Step 2. Send assign requests for all the granules and transfer assignments
	// from local workerAssignments to bmData
	// before we take ownership of all of the ranges, check the manager lock again
	tr->reset();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkManagerLock(tr, bmData));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			if (BM_DEBUG) {
				fmt::print("BM {0} got error checking lock after recovery: {1}\n", bmData->epoch, e.name());
			}
			wait(tr->onError(e));
		}
	}

	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		throw blob_manager_replaced();
	}

	// Get set of workers again. Some could have died after reporting assignments
	std::unordered_set<UID> endingWorkers;
	for (auto& it : bmData->workersById) {
		endingWorkers.insert(it.first);
	}

	// revoke assignments that are old and incorrect
	CODE_PROBE(!outOfDateAssignments.empty(), "BM resolved conflicting assignments on recovery");
	for (auto& it : outOfDateAssignments) {
		if (BM_DEBUG) {
			fmt::print("BM {0} revoking out of date assignment [{1} - {2}): {3}:\n",
			           bmData->epoch,
			           it.second.begin.printable().c_str(),
			           it.second.end.printable().c_str(),
			           it.first.toString().c_str());
		}
		RangeAssignment raRevoke;
		raRevoke.isAssign = false;
		raRevoke.worker = it.first;
		raRevoke.keyRange = it.second;
		raRevoke.revoke = RangeRevokeData(false);
		handleRangeAssign(bmData, raRevoke);
	}

	if (BM_DEBUG) {
		fmt::print("BM {0} final ranges:\n", bmData->epoch);
	}

	state int totalGranules = 0;
	state int explicitAssignments = 0;
	for (auto& range : workerAssignments.intersectingRanges(normalKeys)) {
		int64_t epoch = std::get<1>(range.value());
		int64_t seqno = std::get<2>(range.value());
		if (epoch == 0 && seqno == 0) {
			continue;
		}

		totalGranules++;

		UID workerId = std::get<0>(range.value());
		bmData->workerAssignments.insert(range.range(), workerId);

		if (BM_DEBUG) {
			fmt::print("  [{0} - {1}): {2}\n",
			           range.begin().printable(),
			           range.end().printable(),
			           workerId == UID() || epoch == 0 ? " (?)" : workerId.toString().substr(0, 5).c_str());
		}

		// if worker id is already set to a known worker that replied with it in the mapping, range is already assigned
		// there. If not, need to explicitly assign it to someone
		if (workerId == UID() || epoch == 0 || !endingWorkers.count(workerId)) {
			RangeAssignment raAssign;
			raAssign.isAssign = true;
			raAssign.worker = workerId;
			raAssign.keyRange = range.range();
			raAssign.assign = RangeAssignmentData(AssignRequestType::Normal);
			handleRangeAssign(bmData, raAssign);
			explicitAssignments++;
		}
	}

	// Load tenant data before letting blob granule operations continue.
	tr->reset();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(loadTenantMap(tr, bmData));
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	wait(loadBlobGranuleMergeBoundaries(bmData));

	TraceEvent("BlobManagerRecovered", bmData->id)
	    .detail("Epoch", bmData->epoch)
	    .detail("Duration", now() - recoveryStartTime)
	    .detail("Granules", totalGranules)
	    .detail("Assigned", explicitAssignments)
	    .detail("Revoked", outOfDateAssignments.size());

	ASSERT(bmData->doneRecovering.canBeSet());
	bmData->doneRecovering.send(Void());

	if (BUGGIFY && bmData->maybeInjectTargetedRestart()) {
		throw blob_manager_replaced();
	}

	return Void();
}

ACTOR Future<Void> chaosRangeMover(Reference<BlobManagerData> bmData) {
	// Only move each granule once during the test, otherwise it can cause availability issues
	// KeyRange isn't hashable and this is only for simulation, so just use toString of range
	state std::unordered_set<std::string> alreadyMoved;
	ASSERT(g_network->isSimulated());
	CODE_PROBE(true, "BM chaos range mover enabled");
	loop {
		wait(delay(30.0));

		if (g_simulator->speedUpSimulation) {
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
				if (randomRange.value() != UID() && !alreadyMoved.count(randomRange.range().toString())) {
					if (BM_DEBUG) {
						fmt::print("Range mover moving range [{0} - {1}): {2}\n",
						           randomRange.begin().printable().c_str(),
						           randomRange.end().printable().c_str(),
						           randomRange.value().toString().c_str());
					}
					alreadyMoved.insert(randomRange.range().toString());

					// FIXME: with low probability, could immediately revoke it from the new assignment and move
					// it back right after to test that race

					state KeyRange range = randomRange.range();
					RangeAssignment revokeOld;
					revokeOld.isAssign = false;
					revokeOld.keyRange = range;
					revokeOld.revoke = RangeRevokeData(false);
					handleRangeAssign(bmData, revokeOld);

					RangeAssignment assignNew;
					assignNew.isAssign = true;
					assignNew.keyRange = range;
					assignNew.assign = RangeAssignmentData(); // not a continue
					handleRangeAssign(bmData, assignNew);
					break;
				}
			}
			if (tries == 0 && BM_DEBUG) {
				printf("Range mover couldn't find random range to move, skipping\n");
			}
		} else if (BM_DEBUG) {
			fmt::print("Range mover found {0} workers, skipping\n", bmData->workerAssignments.size());
		}
	}
}

// Returns the number of blob workers on addr
int numExistingBWOnAddr(Reference<BlobManagerData> self, const AddressExclusion& addr) {
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
ACTOR Future<Void> initializeBlobWorker(Reference<BlobManagerData> self, RecruitBlobWorkerReply candidateWorker) {
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

		TraceEvent("BMRecruiting", self->id)
		    .detail("Epoch", self->epoch)
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
			CODE_PROBE(true, "BM got error recruiting BW");
			TraceEvent(SevWarn, "BMRecruitmentError", self->id)
			    .error(newBlobWorker.getError())
			    .detail("Epoch", self->epoch);
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
					self->workerStats[bwi.id()] = BlobWorkerInfo();
					self->addActor.send(monitorBlobWorker(self, bwi));
				} else if (!self->workersById.count(bwi.id())) {
					self->addActor.send(killBlobWorker(self, bwi, false));
				}
			}

			TraceEvent("BMRecruiting", self->id)
			    .detail("Epoch", self->epoch)
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
    Reference<BlobManagerData> self,
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

			TraceEvent("BMRecruiting", self->id).detail("Epoch", self->epoch).detail("State", "Sending request to CC");

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
			CODE_PROBE(true, "Blob worker recruitment timed out");
		}
	}
}

ACTOR Future<Void> haltBlobGranules(Reference<BlobManagerData> bmData) {
	std::vector<BlobWorkerInterface> blobWorkers = wait(getBlobWorkers(bmData->db));
	std::vector<Future<Void>> deregisterBlobWorkers;
	for (auto& worker : blobWorkers) {
		bmData->addActor.send(haltBlobWorker(bmData, worker));
		deregisterBlobWorkers.emplace_back(deregisterBlobWorker(bmData, worker));
	}
	waitForAll(deregisterBlobWorkers);

	return Void();
}

ACTOR Future<GranuleFiles> loadHistoryFiles(Reference<BlobManagerData> bmData, UID granuleID) {
	state Transaction tr(bmData->db);
	state KeyRange range = blobGranuleFileKeyRangeFor(granuleID);
	state Key startKey = range.begin;
	state GranuleFiles files;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(readGranuleFiles(&tr, &startKey, range.end, &files, granuleID));
			return files;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<bool> canDeleteFullGranuleSplit(Reference<BlobManagerData> self, UID granuleId) {
	state Transaction tr(self->db);
	state KeyRange splitRange = blobGranuleSplitKeyRangeFor(granuleId);
	state KeyRange checkRange = splitRange;
	state bool retry = false;

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully delete granule split check {1}\n", self->epoch, granuleId.toString());
	}

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			int lim = SERVER_KNOBS->BG_MAX_SPLIT_FANOUT;
			if (BUGGIFY_WITH_PROB(0.1)) {
				lim = deterministicRandom()->randomInt(1, std::max(2, SERVER_KNOBS->BG_MAX_SPLIT_FANOUT));
			}
			state RangeResult splitState = wait(tr.getRange(checkRange, lim));
			// if first try and empty, splitting state is fully cleaned up
			if (!retry && checkRange == splitRange && splitState.empty() && !splitState.more) {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} Proceed with full deletion, no split state for {1}\n",
					           self->epoch,
					           granuleId.toString());
				}
				return true;
			}
			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0} Full delete check found {1} split states for {2}\n",
				           self->epoch,
				           splitState.size(),
				           granuleId.toString());
			}
			state int i = 0;

			for (; i < splitState.size(); i++) {
				UID parent, child;
				BlobGranuleSplitState st;
				Version v;
				std::tie(parent, child) = decodeBlobGranuleSplitKey(splitState[i].key);
				std::tie(st, v) = decodeBlobGranuleSplitValue(splitState[i].value);
				// if split state is done, this granule has definitely persisted a snapshot
				if (st >= BlobGranuleSplitState::Done) {
					continue;
				}
				// if split state isn't even assigned, this granule has definitely not persisted a snapshot
				if (st <= BlobGranuleSplitState::Initialized) {
					retry = true;
					break;
				}

				ASSERT(st == BlobGranuleSplitState::Assigned);
				// if assigned, granule may or may not have snapshotted. Check files to confirm. Since a re-snapshot is
				// the first file written for a new granule, any files present mean it has re-snapshotted from this
				// granule
				KeyRange granuleFileRange = blobGranuleFileKeyRangeFor(child);
				RangeResult files = wait(tr.getRange(granuleFileRange, 1));
				if (files.empty()) {
					retry = true;
					break;
				}
			}
			if (retry) {
				tr.reset();
				wait(delay(1.0));
				retry = false;
				checkRange = splitRange;
			} else {
				if (splitState.empty() || !splitState.more) {
					break;
				}
				checkRange = KeyRangeRef(keyAfter(splitState.back().key), checkRange.end);
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Full delete check {1} done. Not deleting history key\n", self->epoch, granuleId.toString());
	}
	return false;
}

ACTOR Future<Void> canDeleteFullGranuleMerge(Reference<BlobManagerData> self, Optional<UID> mergeChildId) {
	// if this granule is the parent of a merged granule, it needs to re-snapshot the merged granule before we can
	// delete this one
	if (!mergeChildId.present()) {
		return Void();
	}
	CODE_PROBE(true, "checking canDeleteFullGranuleMerge");

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully delete granule merge check {1}\n", self->epoch, mergeChildId.get().toString());
	}

	state Transaction tr(self->db);
	state KeyRange granuleFileRange = blobGranuleFileKeyRangeFor(mergeChildId.get());
	// loop until granule has snapshotted
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult files = wait(tr.getRange(granuleFileRange, 1));
			if (!files.empty()) {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} Fully delete granule merge check {1} done\n",
					           self->epoch,
					           mergeChildId.get().toString());
				}
				return Void();
			}
			wait(delay(1.0));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<bool> canDeleteFullGranule(Reference<BlobManagerData> self, UID granuleId, Optional<UID> mergeChildId) {
	state Future<bool> split = canDeleteFullGranuleSplit(self, granuleId);
	state Future<Void> merge = canDeleteFullGranuleMerge(self, mergeChildId);

	wait(success(split) && merge);
	bool canDeleteHistory = wait(split);
	return canDeleteHistory;
}

static Future<Void> deleteFile(Reference<BlobConnectionProvider> bstoreProvider, std::string filePath) {
	Reference<BackupContainerFileSystem> bstore = bstoreProvider->getForRead(filePath);
	return bstore->deleteFile(filePath);
}

ACTOR Future<Reference<BlobConnectionProvider>> getBStoreForGranule(Reference<BlobManagerData> self,
                                                                    KeyRange granuleRange) {
	if (self->bstore.isValid()) {
		return self->bstore;
	}
	loop {
		state Reference<GranuleTenantData> data;
		wait(store(data, self->tenantData.getDataForGranule(granuleRange)));
		if (data.isValid()) {
			wait(data->bstoreLoaded.getFuture());
			wait(delay(0));
			return data->bstore;
		} else {
			// race on startup between loading tenant ranges and bgcc/purging. just wait
			wait(delay(0.1));
		}
	}
}

/*
 * Deletes all files pertaining to the granule with id granuleId and
 * also removes the history entry for this granule from the system keyspace
 */
ACTOR Future<Void> fullyDeleteGranule(Reference<BlobManagerData> self,
                                      UID granuleId,
                                      Key historyKey,
                                      Version purgeVersion,
                                      KeyRange granuleRange,
                                      Optional<UID> mergeChildID,
                                      bool force) {
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully deleting granule [{1} - {2}): {3} @ {4}{5}\n",
		           self->epoch,
		           granuleRange.begin.printable(),
		           granuleRange.end.printable(),
		           granuleId.toString(),
		           purgeVersion,
		           force ? " (force)" : "");
	}

	// if granule is still splitting and files are needed for new sub-granules to re-snapshot, we can only partially
	// delete the granule, since we need to keep the last snapshot and deltas for splitting
	// Or, if the granule isn't finalized (still needs the history entry for the old change feed id, because all data
	// from the old change feed hasn't yet been persisted in blob), we can delete the files but need to keep the granule
	// history entry.
	state bool canDeleteHistoryKey;
	if (force) {
		canDeleteHistoryKey = true;
	} else {
		wait(store(canDeleteHistoryKey, canDeleteFullGranule(self, granuleId, mergeChildID)));
	}
	state Reference<BlobConnectionProvider> bstore = wait(getBStoreForGranule(self, granuleRange));

	// get files
	GranuleFiles files = wait(loadHistoryFiles(self->db, granuleId));

	std::vector<Future<Void>> deletions;
	state std::vector<std::string> filesToDelete; // TODO: remove, just for debugging

	for (auto snapshotFile : files.snapshotFiles) {
		std::string fname = snapshotFile.filename;
		deletions.push_back(deleteFile(bstore, fname));
		filesToDelete.emplace_back(fname);
	}

	for (auto deltaFile : files.deltaFiles) {
		std::string fname = deltaFile.filename;
		deletions.push_back(deleteFile(bstore, fname));
		filesToDelete.emplace_back(fname);
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully deleting granule {1}: deleting {2} files\n",
		           self->epoch,
		           granuleId.toString(),
		           filesToDelete.size());
		/*for (auto filename : filesToDelete) {
		    fmt::print(" - {}\n", filename.c_str());
		}*/
	}

	// delete the files before the corresponding metadata.
	// this could lead to dangling pointers in fdb, but this granule should
	// never be read again anyways, and we can clean up the keys the next time around.
	// deleting files before corresponding metadata reduces the # of orphaned files.
	wait(waitForAll(deletions));

	// delete metadata in FDB (history entry and file keys)
	if (BM_PURGE_DEBUG) {
		fmt::print(
		    "BM {0} Fully deleting granule {1}: deleting history and file keys\n", self->epoch, granuleId.toString());
	}

	state Transaction tr(self->db);

	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			KeyRange fileRangeKey = blobGranuleFileKeyRangeFor(granuleId);
			if (canDeleteHistoryKey) {
				tr.clear(historyKey);
			}
			tr.clear(fileRangeKey);
			if (force) {
				// check manager lock to not delete metadata out from under a later recovering manager
				wait(checkManagerLock(&tr, self));
				wait(updateChangeFeed(
				    &tr, granuleIDToCFKey(granuleId), ChangeFeedStatus::CHANGE_FEED_DESTROY, granuleRange));
				tr.clear(blobGranuleLockKeyFor(granuleRange));
				tr.clear(blobGranuleSplitKeyRangeFor(granuleId));
				tr.clear(blobGranuleMergeKeyFor(granuleId));
				// FIXME: also clear merge boundaries!
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Fully deleting granule {1}: success {2}\n",
		           self->epoch,
		           granuleId.toString(),
		           canDeleteHistoryKey ? "" : " ignoring history key!");
	}

	TraceEvent(SevDebug, "GranuleFullPurge", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("GranuleID", granuleId)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("FilesPurged", filesToDelete.size());

	++self->stats.granulesFullyPurged;
	self->stats.filesPurged += filesToDelete.size();

	CODE_PROBE(true, "full granule purged");

	return Void();
}

/*
 * For the granule with id granuleId, finds the first snapshot file at a
 * version <= purgeVersion and deletes all files older than it.
 *
 * Assumption: this granule's startVersion might change because the first snapshot
 * file might be deleted. We will need to ensure we don't rely on the granule's startVersion
 * (that's persisted as part of the key), but rather use the granule's first snapshot's version when needed
 */
ACTOR Future<Void> partiallyDeleteGranule(Reference<BlobManagerData> self,
                                          UID granuleId,
                                          Version purgeVersion,
                                          KeyRange granuleRange) {
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: init\n", self->epoch, granuleId.toString());
	}

	state Reference<BlobConnectionProvider> bstore = wait(getBStoreForGranule(self, granuleRange));

	// get files
	GranuleFiles files = wait(loadHistoryFiles(self->db, granuleId));

	// represents the version of the latest snapshot file in this granule with G.version < purgeVersion
	Version latestSnapshotVersion = invalidVersion;

	state std::vector<Future<Void>> deletions; // deletion work per file
	state std::vector<Key> deletedFileKeys; // keys for deleted files
	state std::vector<std::string> filesToDelete; // TODO: remove evenutally, just for debugging

	// TODO: binary search these snapshot files for latestSnapshotVersion
	for (int idx = files.snapshotFiles.size() - 1; idx >= 0; --idx) {
		// if we already found the latestSnapshotVersion, this snapshot can be deleted
		if (latestSnapshotVersion != invalidVersion) {
			std::string fname = files.snapshotFiles[idx].filename;
			deletions.push_back(deleteFile(bstore, fname));
			deletedFileKeys.emplace_back(blobGranuleFileKeyFor(granuleId, files.snapshotFiles[idx].version, 'S'));
			filesToDelete.emplace_back(fname);
		} else if (files.snapshotFiles[idx].version <= purgeVersion) {
			// otherwise if this is the FIRST snapshot file with version < purgeVersion,
			// then we found our latestSnapshotVersion (FIRST since we are traversing in reverse)
			latestSnapshotVersion = files.snapshotFiles[idx].version;
		}
	}

	if (latestSnapshotVersion == invalidVersion) {
		return Void();
	}

	// delete all delta files older than latestSnapshotVersion
	for (auto deltaFile : files.deltaFiles) {
		// traversing in fwd direction, so stop once we find the first delta file past the latestSnapshotVersion
		if (deltaFile.version > latestSnapshotVersion) {
			break;
		}

		// otherwise deltaFile.version <= latestSnapshotVersion so delete it
		// == should also be deleted because the last delta file before a snapshot would have the same version
		std::string fname = deltaFile.filename;
		deletions.push_back(deleteFile(bstore, fname));
		deletedFileKeys.emplace_back(blobGranuleFileKeyFor(granuleId, deltaFile.version, 'D'));
		filesToDelete.emplace_back(fname);
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: deleting {2} files\n",
		           self->epoch,
		           granuleId.toString(),
		           filesToDelete.size());
		/*for (auto filename : filesToDelete) {
		    fmt::print(" - {0}\n", filename);
		}*/
	}

	// TODO: the following comment relies on the assumption that BWs will not get requests to
	// read data that was already purged. confirm assumption is fine. otherwise, we'd need
	// to communicate with BWs here and have them ack the purgeVersion

	// delete the files before the corresponding metadata.
	// this could lead to dangling pointers in fdb, but we should never read data older than
	// purgeVersion anyways, and we can clean up the keys the next time around.
	// deleting files before corresponding metadata reduces the # of orphaned files.
	wait(waitForAll(deletions));

	// delete metadata in FDB (deleted file keys)
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: deleting file keys\n", self->epoch, granuleId.toString());
	}

	state Transaction tr(self->db);

	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			for (auto& key : deletedFileKeys) {
				tr.clear(key);
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Partially deleting granule {1}: success\n", self->epoch, granuleId.toString());
	}
	TraceEvent(SevDebug, "GranulePartialPurge", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("GranuleID", granuleId)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("FilesPurged", filesToDelete.size());

	++self->stats.granulesPartiallyPurged;
	self->stats.filesPurged += filesToDelete.size();

	CODE_PROBE(true, " partial granule purged");

	return Void();
}

/*
 * This method is used to purge the range [startKey, endKey) at (and including) purgeVersion.
 * To do this, we do a BFS traversal starting at the active granules. Then we classify granules
 * in the history as nodes that can be fully deleted (i.e. their files and history can be deleted)
 * and nodes that can be partially deleted (i.e. some of their files can be deleted).
 * Once all this is done, we finally clear the purgeIntent key, if possible, to indicate we are done
 * processing this purge intent.
 */
ACTOR Future<Void> purgeRange(Reference<BlobManagerData> self, KeyRangeRef range, Version purgeVersion, bool force) {
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} purgeRange starting for range [{1} - {2}) @ purgeVersion={3}, force={4}\n",
		           self->epoch,
		           range.begin.printable(),
		           range.end.printable(),
		           purgeVersion,
		           force);
	}

	TraceEvent("PurgeGranulesBegin", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("Range", range)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Force", force);

	// queue of <range, startVersion, endVersion, mergeChildID> for BFS traversal of history
	state std::queue<std::tuple<KeyRange, Version, Version, Optional<UID>>> historyEntryQueue;

	// stacks of <granuleId, historyKey> and <granuleId> (and mergeChildID) to track which granules to delete
	state std::vector<std::tuple<UID, Key, KeyRange, Optional<UID>>> toFullyDelete;
	state std::vector<std::pair<UID, KeyRange>> toPartiallyDelete;

	// track which granules we have already added to traversal
	// note: (startKey, startVersion) uniquely identifies a granule
	state std::unordered_set<std::pair<std::string, Version>, boost::hash<std::pair<std::string, Version>>> visited;

	// find all active granules (that comprise the range) and add to the queue

	state Transaction tr(self->db);

	if (force) {
		// TODO could clean this up after force purge is done, but it's safer not to
		self->forcePurgingRanges.insert(range, true);
		// set force purged range, to prevent future operations on this range
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				// set force purged range, but don't clear mapping range yet, so that if a new BM recovers in the middle
				// of purging, it still knows what granules to purge
				wait(checkManagerLock(&tr, self));
				// FIXME: need to handle this better if range is unaligned. Need to not truncate existing granules, and
				// instead cover whole of intersecting granules at begin/end
				wait(krmSetRangeCoalescing(&tr, blobGranuleForcePurgedKeys.begin, range, normalKeys, "1"_sr));
				wait(tr.commit());

				if (BUGGIFY && self->maybeInjectTargetedRestart()) {
					wait(delay(0)); // should be cancelled
					ASSERT(false);
				}

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		tr.reset();
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	}

	// if range isn't in known blob ranges, do nothing after writing force purge range to database
	bool anyKnownRanges = false;
	auto knownRanges = self->knownBlobRanges.intersectingRanges(range);
	for (auto& it : knownRanges) {
		if (it.cvalue()) {
			anyKnownRanges = true;
			break;
		}
	}

	if (!anyKnownRanges) {
		CODE_PROBE(true, "skipping purge because not in known blob ranges");
		TraceEvent("PurgeGranulesSkippingUnknownRange", self->id)
		    .detail("Epoch", self->epoch)
		    .detail("Range", range)
		    .detail("PurgeVersion", purgeVersion)
		    .detail("Force", force);
		return Void();
	}

	// wait for all active splits and merges in the range to come to a stop, so no races with purging
	std::vector<Future<Void>> activeBoundaryEvals;
	auto boundaries = self->boundaryEvaluations.intersectingRanges(range);
	for (auto& it : boundaries) {
		auto& f = it.cvalue().inProgress;
		if (f.isValid() && !f.isReady() && !f.isError()) {
			activeBoundaryEvals.push_back(f);
		}
	}

	if (!activeBoundaryEvals.empty()) {
		wait(waitForAll(activeBoundaryEvals));
	}

	// some merges aren't counted in boundary evals, for merge/split race reasons
	while (self->isMergeActive(range)) {
		wait(delayJittered(1.0));
	}

	auto ranges = self->workerAssignments.intersectingRanges(range);
	state std::vector<KeyRange> activeRanges;

	// copy into state variable before waits
	for (auto& it : ranges) {
		activeRanges.push_back(it.range());
	}

	state std::set<Key> knownBoundariesPurged;

	if (force) {
		// revoke range from all active blob workers - AFTER we copy set of active ranges to purge
		// if purge covers multiple blobbified ranges, revoke each separately
		auto knownRanges = self->knownBlobRanges.intersectingRanges(range);
		for (auto& it : knownRanges) {
			if (it.cvalue()) {
				RangeAssignment ra;
				ra.isAssign = false;
				ra.keyRange = range & it.range();
				ra.revoke = RangeRevokeData(true); // dispose=true
				if (ra.keyRange.begin > range.begin) {
					knownBoundariesPurged.insert(ra.keyRange.begin);
				}
				if (ra.keyRange.end < range.end) {
					knownBoundariesPurged.insert(ra.keyRange.end);
				}
				handleRangeAssign(self, ra);
			}
		}
	}

	state int rangeIdx;
	for (rangeIdx = 0; rangeIdx < activeRanges.size(); rangeIdx++) {
		state KeyRange activeRange = activeRanges[rangeIdx];
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0} Checking if active range [{1} - {2}) should be purged\n",
			           self->epoch,
			           activeRange.begin.printable(),
			           activeRange.end.printable());
		}

		// assumption: purge boundaries must respect granule boundaries
		if (activeRange.begin < range.begin || activeRange.end > range.end) {
			TraceEvent(SevWarn, "GranulePurgeRangesUnaligned", self->id)
			    .detail("Epoch", self->epoch)
			    .detail("PurgeRange", range)
			    .detail("GranuleRange", activeRange);
			continue;
		}

		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0} Fetching latest history entry for range [{1} - {2})\n",
					           self->epoch,
					           activeRange.begin.printable(),
					           activeRange.end.printable());
				}
				// FIXME: doing this serially will likely be too slow for large purges
				Optional<GranuleHistory> history = wait(getLatestGranuleHistory(&tr, activeRange));
				// TODO: can we tell from the krm that this range is not valid, so that we don't need to do a
				// get
				if (history.present()) {
					if (BM_PURGE_DEBUG) {
						fmt::print("BM {0}   Adding range to history queue: [{1} - {2}) @ {3}\n",
						           self->epoch,
						           activeRange.begin.printable(),
						           activeRange.end.printable(),
						           history.get().version);
					}
					visited.insert({ activeRange.begin.toString(), history.get().version });
					historyEntryQueue.push({ activeRange, history.get().version, MAX_VERSION, {} });
				} else if (BM_PURGE_DEBUG) {
					fmt::print("BM {0}   No history for range, ignoring\n", self->epoch);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0} Beginning BFS traversal of {1} history items for range [{2} - {3}) \n",
		           self->epoch,
		           historyEntryQueue.size(),
		           range.begin.printable(),
		           range.end.printable());
	}
	while (!historyEntryQueue.empty()) {
		// process the node at the front of the queue and remove it
		state KeyRange currRange;
		state Version startVersion;
		state Version endVersion;
		state Optional<UID> mergeChildID;
		std::tie(currRange, startVersion, endVersion, mergeChildID) = historyEntryQueue.front();
		historyEntryQueue.pop();

		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0} Processing history node [{1} - {2}) with versions [{3}, {4})\n",
			           self->epoch,
			           currRange.begin.printable(),
			           currRange.end.printable(),
			           startVersion,
			           endVersion);
		}

		// get the persisted history entry for this granule
		state Standalone<BlobGranuleHistoryValue> currHistoryNode;
		state Key historyKey = blobGranuleHistoryKeyFor(currRange, startVersion);
		state bool foundHistory = false;
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				Optional<Value> persistedHistory = wait(tr.get(historyKey));
				if (persistedHistory.present()) {
					currHistoryNode = decodeBlobGranuleHistoryValue(persistedHistory.get());
					foundHistory = true;
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (!foundHistory) {
			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0}  No history for this node, skipping\n", self->epoch);
			}
			continue;
		}

		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0}  Found history entry for this node. It's granuleID is {1}\n",
			           self->epoch,
			           currHistoryNode.granuleID.toString());
		}

		// There are three cases this granule can fall into:
		// - if the granule's end version is at or before the purge version or this is a force delete,
		//   this granule should be completely deleted
		// - else if the startVersion <= purgeVersion, then G.startVersion < purgeVersion < G.endVersion
		//   and so this granule should be partially deleted
		// - otherwise, this granule is active, so don't schedule it for deletion
		if (force || endVersion <= purgeVersion) {
			if (BM_PURGE_DEBUG) {
				fmt::print(
				    "BM {0}   Granule {1} will be FULLY deleted\n", self->epoch, currHistoryNode.granuleID.toString());
			}
			toFullyDelete.push_back({ currHistoryNode.granuleID, historyKey, currRange, mergeChildID });
		} else if (startVersion < purgeVersion) {
			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0}   Granule {1} will be partially deleted\n",
				           self->epoch,
				           currHistoryNode.granuleID.toString());
			}
			toPartiallyDelete.push_back({ currHistoryNode.granuleID, currRange });
		}

		// add all of the node's parents to the queue
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0}   Checking {1} parents\n", self->epoch, currHistoryNode.parentVersions.size());
		}
		Optional<UID> mergeChildID2 =
		    currHistoryNode.parentVersions.size() > 1 ? currHistoryNode.granuleID : Optional<UID>();
		for (int i = 0; i < currHistoryNode.parentVersions.size(); i++) {
			// for (auto& parent : currHistoryNode.parentVersions.size()) {
			// if we already added this node to queue, skip it; otherwise, mark it as visited
			KeyRangeRef parentRange(currHistoryNode.parentBoundaries[i], currHistoryNode.parentBoundaries[i + 1]);
			Version parentVersion = currHistoryNode.parentVersions[i];
			std::string beginStr = parentRange.begin.toString();
			if (!visited.insert({ beginStr, parentVersion }).second) {
				if (BM_PURGE_DEBUG) {
					fmt::print("BM {0}     Already added [{1} - {2}) @ {3} - {4} to queue, so skipping it\n",
					           self->epoch,
					           parentRange.begin.printable(),
					           parentRange.end.printable(),
					           parentVersion,
					           startVersion);
				}
				continue;
			}

			if (BM_PURGE_DEBUG) {
				fmt::print("BM {0}     Adding parent [{1} - {2}) @ {3} - {4} to queue\n",
				           self->epoch,
				           parentRange.begin.printable(),
				           parentRange.end.printable(),
				           parentVersion,
				           startVersion);
			}

			// the parent's end version is this node's startVersion,
			// since this node must have started where it's parent finished
			historyEntryQueue.push({ parentRange, parentVersion, startVersion, mergeChildID2 });
		}
	}

	// The top of the stacks have the oldest ranges. This implies that for a granule located at
	// index i, it's parent must be located at some index j, where j > i. For this reason,
	// we delete granules in reverse order; this way, we will never end up with unreachable
	// nodes in the persisted history. Moreover, for any node that must be fully deleted,
	// any node that must be partially deleted must occur later on in the history. Thus,
	// we delete the 'toFullyDelete' granules first.
	//
	// Unfortunately we can't do parallelize _full_ deletions because they might
	// race and we'll end up with unreachable nodes in the case of a crash.
	// Since partial deletions only occur for "leafs", they can be done in parallel
	//
	// Note about file deletions: although we might be retrying a deletion of a granule,
	// we won't run into any issues with trying to "re-delete" a blob file since deleting
	// a file that doesn't exist is considered successful

	TraceEvent("PurgeGranulesTraversalComplete", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("Range", range)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Force", force)
	    .detail("VisitedCount", visited.size())
	    .detail("DeletingFullyCount", toFullyDelete.size())
	    .detail("DeletingPartiallyCount", toPartiallyDelete.size());

	state std::vector<Future<Void>> partialDeletions;
	state int i;
	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0}: {1} granules to fully delete\n", self->epoch, toFullyDelete.size());
	}
	// Go backwards through set of granules to guarantee deleting oldest first. This avoids orphaning granules in the
	// deletion process
	// FIXME: could track explicit parent dependencies and parallelize so long as a parent and child aren't running in
	// parallel, but that's non-trivial
	for (i = toFullyDelete.size() - 1; i >= 0; --i) {
		state UID granuleId;
		Key historyKey;
		KeyRange keyRange;
		Optional<UID> mergeChildId;
		std::tie(granuleId, historyKey, keyRange, mergeChildId) = toFullyDelete[i];
		// FIXME: consider batching into a single txn (need to take care of txn size limit)
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0}: About to fully delete granule {1}\n", self->epoch, granuleId.toString());
		}
		wait(fullyDeleteGranule(self, granuleId, historyKey, purgeVersion, keyRange, mergeChildId, force));
		if (BUGGIFY && self->maybeInjectTargetedRestart()) {
			wait(delay(0)); // should be cancelled
			ASSERT(false);
		}
	}

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0}: {1} granules to partially delete\n", self->epoch, toPartiallyDelete.size());
	}

	for (i = toPartiallyDelete.size() - 1; i >= 0; --i) {
		UID granuleId;
		KeyRange keyRange;
		std::tie(granuleId, keyRange) = toPartiallyDelete[i];
		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0}: About to partially delete granule {1}\n", self->epoch, granuleId.toString());
		}
		partialDeletions.emplace_back(partiallyDeleteGranule(self, granuleId, purgeVersion, keyRange));
	}

	wait(waitForAll(partialDeletions));

	if (force) {
		tr.reset();
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		loop {
			try {
				// clear mapping range, so that a new BM doesn't try to recover force purged granules, and clients can't
				// read them
				wait(checkManagerLock(&tr, self));
				wait(krmSetRange(&tr, blobGranuleMappingKeys.begin, range, blobGranuleMappingValueFor(UID())));
				// FIXME: there is probably a cleaner fix than setting extra keys in the database if someone does a
				// purge that's not aligned to boundaries
				for (auto& it : knownBoundariesPurged) {
					// keep original bounds in granule mapping as to not confuse future managers on recovery
					tr.set(it.withPrefix(blobGranuleMappingKeys.begin), blobGranuleMappingValueFor(UID()));
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Now that all the necessary granules and their files have been deleted, we can
	// clear the purgeIntent key to signify that the work is done. However, there could have been
	// another purgeIntent that got written for this table while we were processing this one.
	// If that is the case, we should not clear the key. Otherwise, we can just clear the key.

	if (BM_PURGE_DEBUG) {
		fmt::print("BM {0}: Successfully purged range [{1} - {2}) at purgeVersion={3}\n",
		           self->epoch,
		           range.begin.printable(),
		           range.end.printable(),
		           purgeVersion);
	}

	TraceEvent("PurgeGranulesComplete", self->id)
	    .detail("Epoch", self->epoch)
	    .detail("Range", range)
	    .detail("PurgeVersion", purgeVersion)
	    .detail("Force", force);

	CODE_PROBE(true, "range purge complete");

	++self->stats.purgesProcessed;
	return Void();
}

/*
 * This monitor watches for changes to a key K that gets updated whenever there is a new purge intent.
 * On this change, we scan through all blobGranulePurgeKeys (which look like <startKey, endKey>=<purge_version,
 * force>) and purge any intents.
 *
 * Once the purge has succeeded, we clear the key IF the version is still the same one that was purged.
 * That way, if another purge intent arrived for the same range while we were working on an older one,
 * we wouldn't end up clearing the intent.
 *
 * When watching for changes, we might end up in scenarios where we failed to do the work
 * for a purge intent even though the watch was triggered (maybe the BM had a blip). This is problematic
 * if the intent is a force and there isn't another purge intent for quite some time. To remedy this,
 * if we don't see a watch change in X (configurable) seconds, we will just sweep through the purge intents,
 * consolidating any work we might have missed before.
 *
 * Note: we could potentially use a changefeed here to get the exact purgeIntent that was added
 * rather than iterating through all of them, but this might have too much overhead for latency
 * improvements we don't really need here (also we need to go over all purge intents anyways in the
 * case that the timer is up before any new purge intents arrive).
 */
ACTOR Future<Void> monitorPurgeKeys(Reference<BlobManagerData> self) {
	self->initBStore();

	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

		// Wait for the watch to change, or some time to expire (whichever comes first)
		// before checking through the purge intents. We write a UID into the change key value
		// so that we can still recognize when the watch key has been changed while we weren't
		// monitoring it

		state Key lastPurgeKey = blobGranulePurgeKeys.begin;

		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state std::vector<Future<Void>> purges;
			state CoalescedKeyRangeMap<std::pair<Version, bool>> purgeMap;
			purgeMap.insert(allKeys, std::make_pair<Version, bool>(0, false));
			try {
				// TODO: replace 10000 with a knob
				state RangeResult purgeIntents = wait(tr->getRange(blobGranulePurgeKeys, BUGGIFY ? 1 : 10000));
				if (purgeIntents.size()) {
					CODE_PROBE(true, "BM found purges to process");
					int rangeIdx = 0;
					for (; rangeIdx < purgeIntents.size(); ++rangeIdx) {
						Version purgeVersion;
						KeyRange range;
						bool force;
						std::tie(purgeVersion, range, force) =
						    decodeBlobGranulePurgeValue(purgeIntents[rangeIdx].value);
						auto ranges = purgeMap.intersectingRanges(range);
						bool foundConflict = false;
						for (auto it : ranges) {
							if ((it.value().second && !force && it.value().first < purgeVersion) ||
							    (!it.value().second && force && purgeVersion < it.value().first)) {
								foundConflict = true;
								break;
							}
						}
						if (foundConflict) {
							break;
						}
						purgeMap.insert(range, std::make_pair(purgeVersion, force));

						if (BM_PURGE_DEBUG) {
							fmt::print("BM {0} about to purge range [{1} - {2}) @ {3}, force={4}\n",
							           self->epoch,
							           range.begin.printable(),
							           range.end.printable(),
							           purgeVersion,
							           force ? "T" : "F");
						}
					}
					lastPurgeKey = purgeIntents[rangeIdx - 1].key;

					for (auto it : purgeMap.ranges()) {
						if (it.value().first > 0) {
							purges.emplace_back(purgeRange(self, it.range(), it.value().first, it.value().second));
						}
					}

					// wait for this set of purges to complete before starting the next ones since if we
					// purge a range R at version V and while we are doing that, the time expires, we will
					// end up trying to purge the same range again since the work isn't finished and the
					// purges will race
					//
					// TODO: this isn't that efficient though. Instead we could keep metadata as part of the
					// BM's memory that tracks which purges are active. Once done, we can mark that work as
					// done. If the BM fails then all purges will fail and so the next BM will have a clear
					// set of metadata (i.e. no work in progress) so we will end up doing the work in the
					// new BM

					wait(waitForAll(purges));
					break;
				} else {
					state Future<Void> watchPurgeIntentsChange = tr->watch(blobGranulePurgeChangeKey);
					wait(tr->commit());
					wait(watchPurgeIntentsChange);
					tr->reset();
				}
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		tr->reset();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->clear(KeyRangeRef(blobGranulePurgeKeys.begin, keyAfter(lastPurgeKey)));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		if (BM_PURGE_DEBUG) {
			fmt::print("BM {0} Done clearing current set of purge intents.\n", self->epoch);
		}

		CODE_PROBE(true, "BM finished processing purge intents");
	}
}

ACTOR Future<Void> doLockChecks(Reference<BlobManagerData> bmData) {
	loop {
		Promise<Void> check = bmData->doLockCheck;
		wait(check.getFuture());
		wait(delay(0.5)); // don't do this too often if a lot of conflict

		CODE_PROBE(true, "BM doing lock checks after getting conflicts");

		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(bmData->db);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(checkManagerLock(tr, bmData));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				if (e.code() == error_code_granule_assignment_conflict) {
					if (BM_DEBUG) {
						fmt::print("BM {0} got lock out of date in lock check on conflict! Dying\n", bmData->epoch);
					}
					if (bmData->iAmReplaced.canBeSet()) {
						bmData->iAmReplaced.send(Void());
					}
					return Void();
				}
				wait(tr->onError(e));
				if (BM_DEBUG) {
					fmt::print("BM {0} still ok after checking lock on conflict\n", bmData->epoch);
				}
			}
		}
		bmData->doLockCheck = Promise<Void>();
	}
}

static void blobManagerExclusionSafetyCheck(Reference<BlobManagerData> self,
                                            BlobManagerExclusionSafetyCheckRequest req) {
	TraceEvent("BMExclusionSafetyCheckBegin", self->id).log();
	BlobManagerExclusionSafetyCheckReply reply(true);
	// make sure at least one blob worker remains after exclusions
	if (self->workersById.empty()) {
		TraceEvent("BMExclusionSafetyCheckNoWorkers", self->id).log();
		reply.safe = false;
	} else {
		std::set<UID> remainingWorkers;
		for (auto& worker : self->workersById) {
			remainingWorkers.insert(worker.first);
		}
		for (const AddressExclusion& excl : req.exclusions) {
			for (auto& worker : self->workersById) {
				if (excl.excludes(worker.second.address())) {
					remainingWorkers.erase(worker.first);
				}
			}
		}

		TraceEvent("BMExclusionSafetyChecked", self->id).detail("RemainingWorkers", remainingWorkers.size()).log();
		reply.safe = !remainingWorkers.empty();
	}

	TraceEvent("BMExclusionSafetyCheckEnd", self->id).log();
	req.reply.send(reply);
}

ACTOR Future<int64_t> bgccCheckGranule(Reference<BlobManagerData> bmData, KeyRange range) {
	state std::pair<RangeResult, Version> fdbResult = wait(readFromFDB(bmData->db, range));
	state Reference<BlobConnectionProvider> bstore = wait(getBStoreForGranule(bmData, range));

	std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blobResult =
	    wait(readFromBlob(bmData->db, bstore, range, 0, fdbResult.second));

	if (!compareFDBAndBlob(fdbResult.first, blobResult, range, fdbResult.second, BM_DEBUG)) {
		++bmData->stats.ccMismatches;
	}

	int64_t bytesRead = fdbResult.first.expectedSize();

	++bmData->stats.ccGranulesChecked;
	bmData->stats.ccRowsChecked += fdbResult.first.size();
	bmData->stats.ccBytesChecked += bytesRead;

	return bytesRead;
}

// Check if there is any pending split. It's a precheck for manifest backup
ACTOR Future<bool> hasPendingSplit(Reference<BlobManagerData> self) {
	state Transaction tr(self->db);
	loop {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			RangeResult result = wait(tr.getRange(blobGranuleSplitKeys, GetRangeLimits::BYTE_LIMIT_UNLIMITED));
			for (auto& row : result) {
				std::pair<BlobGranuleSplitState, Version> gss = decodeBlobGranuleSplitValue(row.value);
				if (gss.first != BlobGranuleSplitState::Done) {
					return true;
				}
			}
			return false;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// FIXME: could eventually make this more thorough by storing some state in the DB or something
// FIXME: simpler solution could be to shuffle ranges
ACTOR Future<Void> bgConsistencyCheck(Reference<BlobManagerData> bmData) {
	state Reference<IRateControl> rateLimiter =
	    Reference<IRateControl>(new SpeedLimit(SERVER_KNOBS->BG_CONSISTENCY_CHECK_TARGET_SPEED_KB * 1024, 1));

	if (BM_DEBUG) {
		fmt::print("BGCC starting\n");
	}

	loop {
		if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
			if (BM_DEBUG) {
				printf("BGCC stopping\n");
			}
			return Void();
		}

		if (bmData->workersById.size() >= 1) {
			int tries = 10;
			state KeyRange range;
			while (tries > 0) {
				auto randomRange = bmData->workerAssignments.randomRange();
				if (randomRange.value() != UID()) {
					range = randomRange.range();
					break;
				}
				tries--;
			}

			state int64_t allowanceBytes = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
			if (tries == 0) {
				if (BM_DEBUG) {
					printf("BGCC couldn't find random range to check, skipping\n");
				}
			} else {
				try {
					Optional<int64_t> bytesRead =
					    wait(timeout(bgccCheckGranule(bmData, range), SERVER_KNOBS->BGCC_TIMEOUT));
					if (bytesRead.present()) {
						allowanceBytes = bytesRead.get();
					} else {
						++bmData->stats.ccTimeouts;
					}
				} catch (Error& e) {
					if (e.code() == error_code_operation_cancelled) {
						throw e;
					}
					TraceEvent(SevWarn, "BGCCError", bmData->id).error(e).detail("Epoch", bmData->epoch);
					++bmData->stats.ccErrors;
				}
			}
			// wait at least some interval if snapshot is small and to not overwhelm the system with reads (for example,
			// empty database with one empty granule)
			wait(rateLimiter->getAllowance(allowanceBytes) && delay(SERVER_KNOBS->BGCC_MIN_INTERVAL));
		} else {
			if (BM_DEBUG) {
				fmt::print("BGCC found no workers, skipping\n", bmData->workerAssignments.size());
			}
			wait(delay(60.0));
		}
	}
}

ACTOR Future<Void> backupManifest(Reference<BlobManagerData> bmData) {
	if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
		return Void();
	}

	bmData->initBStore();
	loop {
		bool pendingSplit = wait(hasPendingSplit(bmData));
		if (!pendingSplit) {
			wait(dumpManifest(bmData->db, bmData->bstore, bmData->epoch, bmData->manifestDumperSeqNo));
			bmData->manifestDumperSeqNo++;
		}
		wait(delay(SERVER_KNOBS->BLOB_MANIFEST_BACKUP_INTERVAL));
	}
}

// Simulation validation that multiple blob managers aren't started with the same epoch
static std::map<int64_t, UID> managerEpochsSeen;

ACTOR Future<Void> checkBlobManagerEpoch(Reference<AsyncVar<ServerDBInfo> const> dbInfo, int64_t epoch, UID dbgid) {
	loop {
		if (dbInfo->get().blobManager.present() && dbInfo->get().blobManager.get().epoch > epoch) {
			throw worker_removed();
		}
		wait(dbInfo->onChange());
	}
}

ACTOR Future<Void> blobManager(BlobManagerInterface bmInterf,
                               Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                               int64_t epoch) {
	if (g_network->isSimulated()) {
		bool managerEpochAlreadySeen = managerEpochsSeen.count(epoch);
		if (managerEpochAlreadySeen) {
			TraceEvent(SevError, "DuplicateBlobManagersAtEpoch")
			    .detail("Epoch", epoch)
			    .detail("BMID1", bmInterf.id())
			    .detail("BMID2", managerEpochsSeen.at(epoch));
		}
		ASSERT(!managerEpochAlreadySeen);
		managerEpochsSeen[epoch] = bmInterf.id();
	}
	state Reference<BlobManagerData> self =
	    makeReference<BlobManagerData>(bmInterf.id(),
	                                   dbInfo,
	                                   openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True),
	                                   bmInterf.locality.dcId(),
	                                   epoch);

	state Future<Void> collection = actorCollection(self->addActor.getFuture());

	if (BM_DEBUG) {
		fmt::print("Blob manager {0} starting...\n", epoch);
	}
	TraceEvent("BlobManagerInit", bmInterf.id()).detail("Epoch", epoch).log();

	self->epoch = epoch;

	try {
		// although we start the recruiter, we wait until existing workers are ack'd
		auto recruitBlobWorker = IAsyncListener<RequestStream<RecruitBlobWorkerRequest>>::create(
		    dbInfo, [](auto const& info) { return info.clusterInterface.recruitBlobWorker; });

		self->addActor.send(blobWorkerRecruiter(self, recruitBlobWorker));
		self->addActor.send(checkBlobManagerEpoch(dbInfo, epoch, bmInterf.id()));

		// we need to recover the old blob manager's state (e.g. granule assignments) before
		// before the new blob manager does anything
		wait(recoverBlobManager(self) || collection);

		self->addActor.send(doLockChecks(self));
		self->addActor.send(monitorClientRanges(self));
		self->addActor.send(monitorTenants(self));
		self->addActor.send(monitorPurgeKeys(self));
		if (SERVER_KNOBS->BG_CONSISTENCY_CHECK_ENABLED) {
			self->addActor.send(bgConsistencyCheck(self));
		}
		if (SERVER_KNOBS->BG_ENABLE_MERGING) {
			self->addActor.send(granuleMergeChecker(self));
		}
		if (SERVER_KNOBS->BLOB_MANIFEST_BACKUP && !isFullRestoreMode()) {
			self->addActor.send(backupManifest(self));
		}

		if (BUGGIFY) {
			self->addActor.send(chaosRangeMover(self));
		}

		loop choose {
			when(wait(self->iAmReplaced.getFuture())) {
				if (BM_DEBUG) {
					fmt::print("BM {} exiting because it is replaced\n", self->epoch);
				}
				TraceEvent("BlobManagerReplaced", bmInterf.id()).detail("Epoch", epoch);
				break;
			}
			when(HaltBlobManagerRequest req = waitNext(bmInterf.haltBlobManager.getFuture())) {
				req.reply.send(Void());
				TraceEvent("BlobManagerHalted", bmInterf.id()).detail("Epoch", epoch).detail("ReqID", req.requesterID);
				break;
			}
			when(state HaltBlobGranulesRequest req = waitNext(bmInterf.haltBlobGranules.getFuture())) {
				wait(haltBlobGranules(self) || collection);
				req.reply.send(Void());
				TraceEvent("BlobGranulesHalted", bmInterf.id()).detail("Epoch", epoch).detail("ReqID", req.requesterID);
				break;
			}
			when(BlobManagerExclusionSafetyCheckRequest req = waitNext(bmInterf.blobManagerExclCheckReq.getFuture())) {
				blobManagerExclusionSafetyCheck(self, req);
			}
			when(BlobManagerBlockedRequest req = waitNext(bmInterf.blobManagerBlockedReq.getFuture())) {
				req.reply.send(BlobManagerBlockedReply(self->stats.blockedAssignments));
			}
			when(wait(collection)) {
				TraceEvent(SevError, "BlobManagerActorCollectionError");
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& err) {
		TraceEvent("BlobManagerDied", bmInterf.id()).errorUnsuppressed(err);
	}
	// prevent a reference counting cycle
	self->assignsInProgress = KeyRangeActorMap();
	self->boundaryEvaluations.clear();
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
// DB has [B - C). It should show up in knownBlobRanges, [B - C) should be in added, and [A - B) and [C - D)
// should be in removed.
TEST_CASE("/blobmanager/updateranges") {
	KeyRangeMap<bool> knownBlobRanges(false, normalKeys.end);
	Arena ar;

	VectorRef<KeyRangeRef> added;
	VectorRef<KeyRangeRef> removed;

	RangeResult dbDataEmpty;
	std::vector<std::pair<KeyRangeRef, bool>> kbrRanges;

	StringRef keyA = StringRef(ar, "A"_sr);
	StringRef keyB = StringRef(ar, "B"_sr);
	StringRef keyC = StringRef(ar, "C"_sr);
	StringRef keyD = StringRef(ar, "D"_sr);

	// db data setup
	RangeResult dbDataAB;
	dbDataAB.emplace_back(ar, keyA, blobRangeActive);
	dbDataAB.emplace_back(ar, keyB, blobRangeInactive);

	RangeResult dbDataAC;
	dbDataAC.emplace_back(ar, keyA, blobRangeActive);
	dbDataAC.emplace_back(ar, keyC, blobRangeInactive);

	RangeResult dbDataAD;
	dbDataAD.emplace_back(ar, keyA, blobRangeActive);
	dbDataAD.emplace_back(ar, keyD, blobRangeInactive);

	RangeResult dbDataBC;
	dbDataBC.emplace_back(ar, keyB, blobRangeActive);
	dbDataBC.emplace_back(ar, keyC, blobRangeInactive);

	RangeResult dbDataBD;
	dbDataBD.emplace_back(ar, keyB, blobRangeActive);
	dbDataBD.emplace_back(ar, keyD, blobRangeInactive);

	RangeResult dbDataCD;
	dbDataCD.emplace_back(ar, keyC, blobRangeActive);
	dbDataCD.emplace_back(ar, keyD, blobRangeInactive);

	RangeResult dbDataAB_CD;
	dbDataAB_CD.emplace_back(ar, keyA, blobRangeActive);
	dbDataAB_CD.emplace_back(ar, keyB, blobRangeInactive);
	dbDataAB_CD.emplace_back(ar, keyC, blobRangeActive);
	dbDataAB_CD.emplace_back(ar, keyD, blobRangeInactive);

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
