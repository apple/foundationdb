/*
 * BlobGranuleValidation.actor.cpp
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

#include "fdbserver/BlobGranuleValidation.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/BlobGranuleRequest.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<std::pair<RangeResult, Version>> readFromFDB(Database cx, KeyRange range) {
	state bool first = true;
	state Version v;
	state RangeResult out;
	state Transaction tr(cx);
	state KeyRange currentRange = range;
	loop {
		tr.setOption(FDBTransactionOptions::RAW_ACCESS);
		// use no-cache as this is either used for test validation, or the blob granule consistency check
		tr.setOption(FDBTransactionOptions::READ_SERVER_SIDE_CACHE_DISABLE);
		try {
			state RangeResult r = wait(tr.getRange(currentRange, CLIENT_KNOBS->TOO_MANY));
			Version grv = wait(tr.getReadVersion());
			// need consistent version snapshot of range
			if (first) {
				v = grv;
				first = false;
			} else if (v != grv) {
				// reset the range and restart the read at a higher version
				first = true;
				out = RangeResult();
				currentRange = range;
				tr.reset();
				continue;
			}
			out.arena().dependsOn(r.arena());
			out.append(out.arena(), r.begin(), r.size());
			if (r.more) {
				currentRange = KeyRangeRef(keyAfter(r.back().key), currentRange.end);
			} else {
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return std::pair(out, v);
}

// FIXME: typedef this pair type and/or chunk list
ACTOR Future<std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>>> readFromBlob(
    Database cx,
    Reference<BlobConnectionProvider> bstore,
    KeyRange range,
    Version beginVersion,
    Version readVersion,
    Optional<Reference<Tenant>> tenant) {
	state RangeResult out;
	state Standalone<VectorRef<BlobGranuleChunkRef>> chunks;
	state Transaction tr(cx, tenant);

	loop {
		try {
			Standalone<VectorRef<BlobGranuleChunkRef>> chunks_ =
			    wait(tr.readBlobGranules(range, beginVersion, readVersion));
			chunks = chunks_;
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	for (const BlobGranuleChunkRef& chunk : chunks) {
		ASSERT(chunk.tenantPrefix.present() == tenant.present());
		RangeResult chunkRows = wait(readBlobGranule(chunk, range, beginVersion, readVersion, bstore));
		out.arena().dependsOn(chunkRows.arena());
		out.append(out.arena(), chunkRows.begin(), chunkRows.size());
	}
	return std::pair(out, chunks);
}

bool compareFDBAndBlob(RangeResult fdb,
                       std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob,
                       KeyRange range,
                       Version v,
                       bool debug) {
	bool correct = fdb == blob.first;
	if (!correct) {
		TraceEvent ev(SevError, "GranuleMismatch");
		ev.detail("RangeStart", range.begin)
		    .detail("RangeEnd", range.end)
		    .detail("Version", v)
		    .detail("FDBSize", fdb.size())
		    .detail("BlobSize", blob.first.size());

		if (debug) {
			fmt::print("\nMismatch for [{0} - {1}) @ {2}. F({3}) B({4}):\n",
			           range.begin.printable(),
			           range.end.printable(),
			           v,
			           fdb.size(),
			           blob.first.size());

			Optional<KeyValueRef> lastCorrect;
			for (int i = 0; i < std::max(fdb.size(), blob.first.size()); i++) {
				if (i >= fdb.size() || i >= blob.first.size() || fdb[i] != blob.first[i]) {
					TraceEvent ev("GranuleMismatchInfo");
					ev.detail("Idx", i);
					printf("  Found mismatch at %d.\n", i);
					if (lastCorrect.present()) {
						printf("    last correct: %s=%s\n",
						       lastCorrect.get().key.printable().c_str(),
						       lastCorrect.get().value.printable().c_str());
						ev.detail("LastCorrectKey", lastCorrect.get().key);
					}
					if (i < fdb.size()) {
						printf("    FDB: %s=%s\n", fdb[i].key.printable().c_str(), fdb[i].value.printable().c_str());
						ev.detail("FDBKey", fdb[i].key);
					} else {
						printf("    FDB: <missing>\n");
						ev.detail("FDBKey", "Missing");
					}
					if (i < blob.first.size()) {
						printf("    BLB: %s=%s\n",
						       blob.first[i].key.printable().c_str(),
						       blob.first[i].value.printable().c_str());
						ev.detail("BlobKey", blob.first[i].key);
					} else {
						printf("    BLB: <missing>\n");
						ev.detail("BlobKey", "Missing");
					}
					if (i < fdb.size() && i < blob.first.size() && fdb[i].key == blob.first[i].key) {
						// value mismatch
						ev.detail("FDBValue", fdb[i].value).detail("BlobValue", blob.first[i].value);
					}
					printf("\n");
					break;
				}
				if (i < fdb.size()) {
					lastCorrect = fdb[i];
				} else {
					lastCorrect = blob.first[i];
				}
			}

			printGranuleChunks(blob.second);
		}
	}
	return correct;
}

void printGranuleChunks(const Standalone<VectorRef<BlobGranuleChunkRef>>& chunks) {
	printf("Chunks:\n");
	for (auto& chunk : chunks) {
		printf("[%s - %s)\n", chunk.keyRange.begin.printable().c_str(), chunk.keyRange.end.printable().c_str());

		printf("  SnapshotFile:\n    %s\n",
		       chunk.snapshotFile.present() ? chunk.snapshotFile.get().toString().c_str() : "<none>");
		printf("  DeltaFiles:\n");
		for (auto& df : chunk.deltaFiles) {
			printf("    %s\n", df.toString().c_str());
		}
		printf("  Deltas: (%d)", chunk.newDeltas.size());
		if (chunk.newDeltas.size() > 0) {
			fmt::print(" with version [{0} - {1}]",
			           chunk.newDeltas[0].version,
			           chunk.newDeltas[chunk.newDeltas.size() - 1].version);
		}
		fmt::print("  IncludedVersion: {}\n", chunk.includedVersion);
	}
	printf("\n");
}

ACTOR Future<Void> clearAndAwaitMerge(Database cx, KeyRange range) {
	// clear key range and check whether it is merged or not, repeatedly
	state Transaction tr(cx);
	state int reClearCount = 1;
	state int reClearInterval = 1; // do quadratic backoff on clear rate, b/c large keys can keep it not write-cold
	loop {
		try {
			Standalone<VectorRef<KeyRangeRef>> ranges = wait(tr.getBlobGranuleRanges(range, 2));
			if (ranges.size() == 1) {
				return Void();
			}
			CODE_PROBE(true, "ClearAndAwaitMerge doing clear");
			reClearCount--;
			if (reClearCount <= 0) {
				tr.clear(range);
				wait(tr.commit());
				fmt::print("ClearAndAwaitMerge cleared [{0} - {1}) @ {2}\n",
				           range.begin.printable(),
				           range.end.printable(),
				           tr.getCommittedVersion());
				reClearCount = reClearInterval;
				reClearInterval++;
			}
			wait(delay(30.0)); // sleep a bit before checking on merge again
			tr.reset();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Standalone<VectorRef<BlobGranuleSummaryRef>>> getSummaries(Database cx,
                                                                        KeyRange range,
                                                                        Version summaryVersion,
                                                                        Optional<Reference<Tenant>> tenant) {
	state Transaction tr(cx, tenant);
	loop {
		try {
			Standalone<VectorRef<BlobGranuleSummaryRef>> summaries =
			    wait(tr.summarizeBlobGranules(range, summaryVersion, 1000000));

			// do some basic validation
			ASSERT(!summaries.empty());
			ASSERT(summaries.front().keyRange.begin == range.begin);
			ASSERT(summaries.back().keyRange.end == range.end);

			for (int i = 0; i < summaries.size() - 1; i++) {
				ASSERT(summaries[i].keyRange.end == summaries[i + 1].keyRange.begin);
			}

			return summaries;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> validateGranuleSummaries(Database cx,
                                            KeyRange range,
                                            Optional<Reference<Tenant>> tenant,
                                            Promise<Void> testComplete) {
	state Arena lastSummaryArena;
	state KeyRangeMap<Optional<BlobGranuleSummaryRef>> lastSummary;
	state Version lastSummaryVersion = invalidVersion;
	state Transaction tr(cx, tenant);
	state int successCount = 0;
	try {
		loop {
			// get grv and get latest summaries
			state Version nextSummaryVersion;
			tr.reset();
			loop {
				try {
					wait(store(nextSummaryVersion, tr.getReadVersion()));
					ASSERT(nextSummaryVersion >= lastSummaryVersion);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			state Standalone<VectorRef<BlobGranuleSummaryRef>> nextSummary;
			try {
				wait(store(nextSummary, getSummaries(cx, range, nextSummaryVersion, tenant)));
			} catch (Error& e) {
				if (e.code() == error_code_blob_granule_transaction_too_old) {
					ASSERT(lastSummaryVersion == invalidVersion);

					wait(delay(1.0));
					continue;
				} else {
					throw e;
				}
			}

			if (lastSummaryVersion != invalidVersion) {
				CODE_PROBE(true, "comparing multiple summaries");
				// diff with last summary ranges to ensure versions never decreased for any range
				for (auto& it : nextSummary) {
					auto lastSummaries = lastSummary.intersectingRanges(it.keyRange);
					for (auto& itLast : lastSummaries) {

						if (!itLast.cvalue().present()) {
							ASSERT(lastSummaryVersion == invalidVersion);
							continue;
						}
						auto& last = itLast.cvalue().get();

						ASSERT(it.snapshotVersion >= last.snapshotVersion);
						// same invariant isn't always true for delta version because of force flushing around granule
						// merges
						if (it.keyRange == itLast.range()) {
							if (it.snapshotVersion == last.snapshotVersion) {
								ASSERT(it.snapshotSize == last.snapshotSize);
							}
							if (it.snapshotVersion == last.snapshotVersion && it.deltaVersion == last.deltaVersion) {
								ASSERT(it.snapshotSize == last.snapshotSize);
								ASSERT(it.deltaSize == last.deltaSize);
							} else if (it.snapshotVersion == last.snapshotVersion) {
								// empty delta files can cause version to decrease or size to remain same with a version
								// increase
								if (it.deltaVersion >= last.deltaVersion) {
									ASSERT(it.deltaSize >= last.deltaSize);
								} // else can happen because of empty delta file version bump
							}
							break;
						}
					}
				}

				if (!testComplete.canBeSet()) {
					return Void();
				}
			}

			successCount++;

			lastSummaryArena = nextSummary.arena();
			lastSummaryVersion = nextSummaryVersion;
			lastSummary.insert(range, {});
			for (auto& it : nextSummary) {
				lastSummary.insert(it.keyRange, it);
			}

			wait(delayJittered(deterministicRandom()->randomInt(1, 10)));
		}
	} catch (Error& e) {
		if (e.code() != error_code_operation_cancelled) {
			TraceEvent(SevError, "UnexpectedErrorValidateGranuleSummaries").error(e);
		}
		throw e;
	}
}

ACTOR Future<Void> validateForceFlushing(Database cx,
                                         KeyRange range, // raw key range (includes tenant)
                                         double testDuration,
                                         Promise<Void> testComplete) {
	TraceEvent("ValidateForceFlushSleeping").detail("Range", range);
	// do randomly once with random delay through whole test
	wait(delay(deterministicRandom()->random01() * testDuration));

	TraceEvent("ValidateForceFlushRunning").detail("Range", range);

	// verify range first to make sure it's active
	loop {
		Version v = wait(cx->verifyBlobRange(range, {}, {}));
		if (v != invalidVersion) {
			TraceEvent("ValidateForceFlushVerified").detail("Range", range).detail("Version", v);
			break;
		}
		wait(delay(2.0));
	}

	state KeyRange toFlush = range;
	state Version flushVersion = invalidVersion;
	// then get blob ranges, pick random set of range(s) within
	state Transaction tr(cx);
	loop {
		tr.setOption(FDBTransactionOptions::RAW_ACCESS);
		try {
			wait(store(flushVersion, tr.getReadVersion()));
			Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(range, 1000));
			ASSERT(!granules.empty());
			if (granules.size() > 2) {
				// pick sub-range if many granules to flush
				int targetRanges = deterministicRandom()->randomInt(1, std::min(10, (int)granules.size()));
				int targetStart = deterministicRandom()->randomInt(0, granules.size() - targetRanges);

				Key startKey = granules[targetStart].begin;
				Key endKey = granules[targetStart + targetRanges - 1].end;

				// client req may not exactly align to granules - buggify this behavior
				if (BUGGIFY_WITH_PROB(0.1)) {
					if (deterministicRandom()->coinflip()) {
						startKey = keyAfter(startKey);
					}
					// extend end (if there are granules in that space)
					if (targetStart + targetRanges < granules.size() && deterministicRandom()->coinflip()) {
						endKey = keyAfter(endKey);
					}
				}

				toFlush = KeyRangeRef(startKey, endKey);
			}

			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	// since this call can come from a client, don't assume that the passed version is a grv/committed version.
	// Buggify to enforce this
	if (BUGGIFY_WITH_PROB(0.1)) {
		flushVersion += deterministicRandom()->randomInt(0, 1000000);
		TraceEvent("ValidateForceFlushAddingJitter")
		    .detail("Range", range)
		    .detail("ToFlush", toFlush)
		    .detail("NewVersion", flushVersion);
	}

	state bool compact = deterministicRandom()->random01() < 0.25;

	TraceEvent("ValidateForceFlushRequesting")
	    .detail("Range", range)
	    .detail("ToFlush", toFlush)
	    .detail("Version", flushVersion)
	    .detail("Compact", compact);

	// call flush and make sure it returns eventually
	FlushGranuleRequest req(-1, toFlush, flushVersion, compact);
	wait(success(doBlobGranuleRequests(cx, toFlush, req, &BlobWorkerInterface::flushGranuleRequest)));

	TraceEvent("ValidateForceFlushRequestComplete")
	    .detail("Range", range)
	    .detail("ToFlush", toFlush)
	    .detail("Version", flushVersion)
	    .detail("Compact", compact);

	// once it returns, do a read from the range at that version and make sure the returned versions all obey the
	// property that at least flushVersion is durable

	// FIXME: just flush in loop until this works to fix merging race?
	state Version readVersion;
	tr.reset();
	loop {
		try {
			tr.setOption(FDBTransactionOptions::RAW_ACCESS);
			if (compact) {
				// read at current read version version in case re-snapshot had to redo at a higher version
				wait(store(readVersion, tr.getReadVersion()));
			} else {
				readVersion = flushVersion;
			}
			Standalone<VectorRef<BlobGranuleChunkRef>> chunks = wait(tr.readBlobGranules(toFlush, 0, readVersion));
			fmt::print("Chunks from force flush [{0} - {1}) @ {2}\n",
			           toFlush.begin.printable(),
			           toFlush.end.printable(),
			           readVersion);
			printGranuleChunks(chunks);
			fmt::print("Processing\n");
			for (auto& it : chunks) {

				if (compact) {
					if (it.snapshotVersion < flushVersion) {
						fmt::print("Chunk [{0} - {1}). SV={2}, FV={3}\n",
						           it.keyRange.begin.printable(),
						           it.keyRange.end.printable(),
						           it.snapshotVersion,
						           flushVersion);
					}
					ASSERT(it.snapshotVersion >= flushVersion);
				} /* else {
				     // TODO this check doesn't work due to race with merging. Just make sure it finishes

				     if (!it.newDeltas.empty()) {
				         fmt::print("{0} Deltas {1} - {2}\n", it.newDeltas.size(), it.newDeltas.front().version,
				 it.newDeltas.back().version);
				     }
				     ASSERT(it.newDeltas.empty());
				     if (it.snapshotVersion < flushVersion) {
				         ASSERT(!it.deltaFiles.empty());
				         // could parse delta file version out of file but that's annoying
				     }
				 }*/
			}

			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	TraceEvent("ValidateForceFlushDone")
	    .detail("Range", range)
	    .detail("ToFlush", toFlush)
	    .detail("Version", flushVersion);

	return Void();
}

struct feed_cmp_f {
	bool operator()(const std::pair<Key, KeyRange>& lhs, const std::pair<Key, KeyRange>& rhs) const {
		if (lhs.second.begin == rhs.second.begin) {
			return lhs.second.end < rhs.second.end;
		}
		return lhs.second.begin < rhs.second.begin;
	}
};

ACTOR Future<std::vector<std::pair<Key, KeyRange>>> getActiveFeeds(Transaction* tr) {
	RangeResult feedResult = wait(tr->getRange(changeFeedKeys, CLIENT_KNOBS->BG_TOO_MANY_GRANULES));
	ASSERT(!feedResult.more);
	std::vector<std::pair<Key, KeyRange>> results;
	for (auto& it : feedResult) {
		Key feedKey = it.key.removePrefix(changeFeedPrefix);
		KeyRange feedRange;
		Version version;
		ChangeFeedStatus status;

		std::tie(feedRange, version, status) = decodeChangeFeedValue(it.value);
		results.push_back({ feedKey, feedRange });
	}

	std::sort(results.begin(), results.end(), feed_cmp_f());

	return results;
}

// TODO: add debug parameter
// FIXME: this check currently assumes blob granules are the only users of change feeds, and will fail if that is not
// the case
ACTOR Future<Void> checkFeedCleanup(Database cx, bool debug) {
	if (SERVER_KNOBS->BLOB_WORKER_FORCE_FLUSH_CLEANUP_DELAY < 0) {
		// no guarantee of feed cleanup, return
		return Void();
	}
	// big extra timeout just because simulation can take a while to quiesce
	state double checkTimeoutOnceStable = 300.0 + 2 * SERVER_KNOBS->BLOB_WORKER_FORCE_FLUSH_CLEANUP_DELAY;
	state Optional<double> stableTimestamp;
	state Standalone<VectorRef<KeyRangeRef>> lastGranules;

	state Transaction tr(cx);
	loop {
		try {
			// get set of current granules. if different than last set of granules
			state Standalone<VectorRef<KeyRangeRef>> granules = wait(tr.getBlobGranuleRanges(normalKeys, 10000));
			state std::vector<std::pair<Key, KeyRange>> activeFeeds = wait(getActiveFeeds(&tr));

			// TODO REMOVE
			if (debug) {
				fmt::print("{0} granules and {1} active feeds found\n", granules.size(), activeFeeds.size());
			}
			/*fmt::print("Granules:\n");
			for (auto& it : granules) {
			    fmt::print("  [{0} - {1})\n", it.begin.printable(), it.end.printable());
			}*/
			bool allPresent = granules.size() == activeFeeds.size();
			for (int i = 0; allPresent && i < granules.size(); i++) {
				if (granules[i] != activeFeeds[i].second) {
					if (debug) {
						fmt::print("Feed {0} for [{1} - {2}) still exists despite no granule!\n",
						           activeFeeds[i].first.printable(),
						           activeFeeds[i].second.begin.printable(),
						           activeFeeds[i].second.end.printable());
					}
					allPresent = false;
					break;
				}
			}
			if (allPresent) {
				if (debug) {
					fmt::print("Feed Cleanup Check Complete\n");
				}
				return Void();
			}
			if (granules != lastGranules) {
				stableTimestamp.reset();
			} else if (!stableTimestamp.present()) {
				stableTimestamp = now();
			}
			lastGranules = granules;

			// ensure this converges within a time window of granules becoming stable
			if (stableTimestamp.present()) {
				ASSERT(now() - stableTimestamp.get() <= checkTimeoutOnceStable);
			}

			wait(delay(2.0));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}
