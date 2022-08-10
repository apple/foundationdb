/*
 * BlobGranuleVerifier.actor.cpp
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

#include <map>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BlobGranuleValidation.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BGV_DEBUG true

/*
 * This workload is designed to verify the correctness of the blob data produced by the blob workers.
 * As a read-only validation workload, it can piggyback off of other write or read/write workloads.
 * To verify the data outside FDB's 5 second MVCC window, it tests time travel reads by doing an initial comparison at
 * the latest read version, and then waiting a period of time to re-read the data from blob.
 * To catch availability issues with the blob worker, it does a request to each granule at the end of the test.
 */
struct BlobGranuleVerifierWorkload : TestWorkload {
	bool doSetup;
	double testDuration;
	double timeTravelLimit;
	uint64_t timeTravelBufferSize;
	int threads;
	int64_t errors = 0;
	int64_t mismatches = 0;
	int64_t initialReads = 0;
	int64_t timeTravelReads = 0;
	int64_t timeTravelTooOld = 0;
	int64_t rowsRead = 0;
	int64_t bytesRead = 0;
	int64_t purges = 0;
	std::vector<Future<Void>> clients;
	bool enablePurging;
	bool initAtEnd;
	bool strictPurgeChecking;
	bool clearAndMergeCheck;

	DatabaseConfiguration config;

	Reference<BlobConnectionProvider> bstore;
	AsyncVar<Standalone<VectorRef<KeyRangeRef>>> granuleRanges;

	BlobGranuleVerifierWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		doSetup = !clientId; // only do this on the "first" client
		testDuration = getOption(options, LiteralStringRef("testDuration"), 120.0);
		timeTravelLimit = getOption(options, LiteralStringRef("timeTravelLimit"), testDuration);
		timeTravelBufferSize = getOption(options, LiteralStringRef("timeTravelBufferSize"), 100000000);
		threads = getOption(options, LiteralStringRef("threads"), 1);
		enablePurging = getOption(options, LiteralStringRef("enablePurging"), false /*sharedRandomNumber % 2 == 0*/);
		sharedRandomNumber /= 2;
		// FIXME: re-enable this! There exist several bugs with purging active granules where a small amount of state
		// won't be cleaned up.
		strictPurgeChecking =
		    getOption(options, LiteralStringRef("strictPurgeChecking"), false /*sharedRandomNumber % 2 == 0*/);
		sharedRandomNumber /= 10;

		// randomly some tests write data first and then turn on blob granules later, to test conversion of existing DB
		initAtEnd = !enablePurging && sharedRandomNumber % 10 == 0;
		sharedRandomNumber /= 10;

		clearAndMergeCheck = getOption(options, LiteralStringRef("clearAndMergeCheck"), sharedRandomNumber % 10 == 0);
		sharedRandomNumber /= 10;

		ASSERT(threads >= 1);

		if (BGV_DEBUG) {
			printf("Initializing Blob Granule Verifier s3 stuff\n");
		}
		try {
			if (BGV_DEBUG) {
				printf("Blob Granule Verifier constructing backup container from %s\n", SERVER_KNOBS->BG_URL.c_str());
			}
			bstore = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
			if (BGV_DEBUG) {
				printf("Blob Granule Verifier constructed backup container\n");
			}
		} catch (Error& e) {
			if (BGV_DEBUG) {
				printf("Blob Granule Verifier got backup container init error %s\n", e.name());
			}
			throw e;
		}
	}

	// FIXME: run the actual FDBCLI command instead of copy/pasting its implementation
	// Sets the whole user keyspace to be blobified
	ACTOR Future<Void> setUpBlobRange(Database cx) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->set(blobRangeChangeKey, deterministicRandom()->randomUniqueID().toString());
				wait(krmSetRange(tr, blobRangeKeys.begin, KeyRange(normalKeys), LiteralStringRef("1")));
				wait(tr->commit());
				if (BGV_DEBUG) {
					printf("Successfully set up blob granule range for normalKeys\n");
				}
				TraceEvent("BlobGranuleVerifierSetup");
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	std::string description() const override { return "BlobGranuleVerifier"; }
	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR Future<Void> _setup(Database cx, BlobGranuleVerifierWorkload* self) {
		if (!self->doSetup) {
			wait(delay(0));
			return Void();
		}

		wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));

		if (!self->initAtEnd) {
			wait(self->setUpBlobRange(cx));
		}
		return Void();
	}

	ACTOR Future<Void> findGranules(Database cx, BlobGranuleVerifierWorkload* self) {
		loop {
			state Transaction tr(cx);
			loop {
				try {
					Standalone<VectorRef<KeyRangeRef>> allGranules = wait(tr.getBlobGranuleRanges(normalKeys));
					self->granuleRanges.set(allGranules);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			wait(delay(deterministicRandom()->random01() * 10.0));
		}
	}

	struct OldRead {
		KeyRange range;
		Version v;
		RangeResult oldResult;

		OldRead() {}
		OldRead(KeyRange range, Version v, RangeResult oldResult) : range(range), v(v), oldResult(oldResult) {}
	};

	ACTOR Future<Void> killBlobWorkers(Database cx, BlobGranuleVerifierWorkload* self) {
		state Transaction tr(cx);
		state std::set<UID> knownWorkers;
		state bool first = true;
		loop {
			try {
				RangeResult r = wait(tr.getRange(blobWorkerListKeys, CLIENT_KNOBS->TOO_MANY));

				state std::vector<UID> haltIds;
				state std::vector<Future<ErrorOr<Void>>> haltRequests;
				for (auto& it : r) {
					BlobWorkerInterface interf = decodeBlobWorkerListValue(it.value);
					if (first) {
						knownWorkers.insert(interf.id());
					}
					if (knownWorkers.count(interf.id())) {
						haltIds.push_back(interf.id());
						haltRequests.push_back(interf.haltBlobWorker.tryGetReply(HaltBlobWorkerRequest(1e6, UID())));
					}
				}
				first = false;
				wait(waitForAll(haltRequests));
				bool allPresent = true;
				for (int i = 0; i < haltRequests.size(); i++) {
					if (haltRequests[i].get().present()) {
						knownWorkers.erase(haltIds[i]);
					} else {
						allPresent = false;
					}
				}
				if (allPresent) {
					return Void();
				} else {
					wait(delay(1.0));
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> verifyGranules(Database cx, BlobGranuleVerifierWorkload* self, bool allowPurging) {
		state double last = now();
		state double endTime = last + self->testDuration;
		state std::map<double, OldRead> timeTravelChecks;
		state int64_t timeTravelChecksMemory = 0;
		state Version prevPurgeVersion = -1;
		state UID dbgId = debugRandom()->randomUniqueID();

		TraceEvent("BlobGranuleVerifierStart");
		if (BGV_DEBUG) {
			printf("BGV thread starting\n");
		}

		// wait for first set of ranges to be loaded
		wait(self->granuleRanges.onChange());

		if (BGV_DEBUG) {
			printf("BGV got ranges\n");
		}

		loop {
			try {
				state double currentTime = now();
				state std::map<double, OldRead>::iterator timeTravelIt = timeTravelChecks.begin();
				while (timeTravelIt != timeTravelChecks.end() && currentTime >= timeTravelIt->first) {
					state OldRead oldRead = timeTravelIt->second;
					timeTravelChecksMemory -= oldRead.oldResult.expectedSize();
					// advance iterator before doing read, so if it gets error we don't retry it
					timeTravelIt = timeTravelChecks.erase(timeTravelIt);
					if (prevPurgeVersion == -1) {
						prevPurgeVersion = oldRead.v;
					}

					// before doing read, purge just before read version
					state Version newPurgeVersion = 0;
					state bool doPurging = allowPurging && deterministicRandom()->random01() < 0.5;
					if (doPurging) {
						CODE_PROBE(true, "BGV considering purge");
						Version maxPurgeVersion = oldRead.v;
						for (auto& it : timeTravelChecks) {
							maxPurgeVersion = std::min(it.second.v, maxPurgeVersion);
						}
						if (prevPurgeVersion < maxPurgeVersion) {
							CODE_PROBE(true, "BGV doing purge");
							newPurgeVersion = deterministicRandom()->randomInt64(prevPurgeVersion, maxPurgeVersion);
							prevPurgeVersion = std::max(prevPurgeVersion, newPurgeVersion);
							if (BGV_DEBUG) {
								fmt::print("BGV Purging @ {0}\n", newPurgeVersion);
							}
							try {
								Key purgeKey = wait(cx->purgeBlobGranules(normalKeys, newPurgeVersion, {}, false));
								if (BGV_DEBUG) {
									fmt::print("BGV Purged @ {0}, waiting\n", newPurgeVersion);
								}
								wait(cx->waitPurgeGranulesComplete(purgeKey));
							} catch (Error& e) {
								if (e.code() == error_code_operation_cancelled) {
									throw e;
								}
								// purging shouldn't error, it should retry.
								if (BGV_DEBUG) {
									fmt::print("Unexpected error {0} purging @ {1}!\n", e.name(), newPurgeVersion);
								}
								ASSERT(false);
							}
							CODE_PROBE(true, "BGV purge complete");
							if (BGV_DEBUG) {
								fmt::print("BGV Purge complete @ {0}\n", newPurgeVersion);
							}
							self->purges++;
						} else {
							doPurging = false;
						}
					}

					// do time travel read
					try {
						std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> reReadResult =
						    wait(readFromBlob(cx, self->bstore, oldRead.range, 0, oldRead.v));
						if (!compareFDBAndBlob(oldRead.oldResult, reReadResult, oldRead.range, oldRead.v, BGV_DEBUG)) {
							self->mismatches++;
						}
						self->timeTravelReads++;
					} catch (Error& e) {
						fmt::print("Error TT: {0}\n", e.name());
						if (e.code() == error_code_blob_granule_transaction_too_old) {
							self->timeTravelTooOld++;
							// TODO: add debugging info for when this is a failure
							fmt::print("BGV ERROR: TTO [{0} - {1}) @ {2}\n",
							           oldRead.range.begin.printable(),
							           oldRead.range.end.printable(),
							           oldRead.v);
						}
					}

					// if purged just before read, verify that purge cleaned up data by restarting blob workers and
					// reading older than the purge version
					if (doPurging) {
						if (self->strictPurgeChecking) {
							wait(self->killBlobWorkers(cx, self));
							if (BGV_DEBUG) {
								fmt::print("BGV Reading post-purge [{0} - {1}) @ {2}\n",
								           oldRead.range.begin.printable(),
								           oldRead.range.end.printable(),
								           prevPurgeVersion);
							}
						}
						// ensure purge version exactly is still readable
						std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> versionRead1 =
						    wait(readFromBlob(cx, self->bstore, oldRead.range, 0, prevPurgeVersion));
						if (BGV_DEBUG) {
							fmt::print("BGV Post-purge first read:\n");
							printGranuleChunks(versionRead1.second);
						}
						if (self->strictPurgeChecking) {
							try {
								// read at purgeVersion - 1, should NOT be readable
								Version minSnapshotVersion = newPurgeVersion;
								for (auto& it : versionRead1.second) {
									minSnapshotVersion = std::min(minSnapshotVersion, it.snapshotVersion);
								}
								if (BGV_DEBUG) {
									fmt::print("BGV Reading post-purge again [{0} - {1}) @ {2}\n",
									           oldRead.range.begin.printable(),
									           oldRead.range.end.printable(),
									           minSnapshotVersion - 1);
								}
								std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> versionRead2 =
								    wait(readFromBlob(cx, self->bstore, oldRead.range, 0, minSnapshotVersion - 1));
								if (BGV_DEBUG) {
									fmt::print("BGV ERROR: data not purged! Read successful!!\n");
									printGranuleChunks(versionRead2.second);
								}
								ASSERT(false);
							} catch (Error& e) {
								if (e.code() == error_code_actor_cancelled) {
									throw;
								}
								ASSERT(e.code() == error_code_blob_granule_transaction_too_old);
								CODE_PROBE(true, "BGV verified too old after purge");
							}
						}
					}
				}

				// pick a random range
				int rIndex = deterministicRandom()->randomInt(0, self->granuleRanges.get().size());
				state KeyRange range = self->granuleRanges.get()[rIndex];

				state std::pair<RangeResult, Version> fdb = wait(readFromFDB(cx, range));
				std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob =
				    wait(readFromBlob(cx, self->bstore, range, 0, fdb.second));
				if (compareFDBAndBlob(fdb.first, blob, range, fdb.second, BGV_DEBUG)) {
					// TODO: bias for immediately re-reading to catch rollback cases
					double reReadTime = currentTime + deterministicRandom()->random01() * self->timeTravelLimit;
					int memory = fdb.first.expectedSize();
					if (reReadTime <= endTime &&
					    timeTravelChecksMemory + memory <= (self->timeTravelBufferSize / self->threads)) {
						timeTravelChecks[reReadTime] = OldRead(range, fdb.second, fdb.first);
						timeTravelChecksMemory += memory;
					}
				} else {
					self->mismatches++;
				}
				self->rowsRead += fdb.first.size();
				self->bytesRead += fdb.first.expectedSize();
				self->initialReads++;

			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw;
				}
				if (e.code() != error_code_blob_granule_transaction_too_old && BGV_DEBUG) {
					printf("BGVerifier got unexpected error %s\n", e.name());
				}
				self->errors++;
			}
			// wait(poisson(&last, 5.0));
			wait(poisson(&last, 0.1));
		}
	}

	Future<Void> start(Database const& cx) override {
		clients.reserve(threads + 1);
		clients.push_back(timeout(findGranules(cx, this), testDuration, Void()));
		if (enablePurging && clientId == 0) {
			clients.push_back(
			    timeout(reportErrors(verifyGranules(cx, this, true), "BlobGranuleVerifier"), testDuration, Void()));
		} else if (!enablePurging) {
			for (int i = 0; i < threads; i++) {
				clients.push_back(timeout(
				    reportErrors(verifyGranules(cx, this, false), "BlobGranuleVerifier"), testDuration, Void()));
			}
		}
		return delay(testDuration);
	}

	// handle retries + errors
	// It's ok to reset the transaction here because its read version is only used for reading the granule mapping from
	// the system keyspace
	ACTOR Future<Version> doGrv(Transaction* tr) {
		loop {
			try {
				Version readVersion = wait(tr->getReadVersion());
				return readVersion;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR Future<bool> _check(Database cx, BlobGranuleVerifierWorkload* self) {
		// check error counts, and do an availability check at the end

		if (self->doSetup && self->initAtEnd) {
			// FIXME: this doesn't check the data contents post-conversion, just that it finishes successfully
			wait(self->setUpBlobRange(cx));
		}

		state Transaction tr(cx);
		state Version readVersion = wait(self->doGrv(&tr));
		state Version startReadVersion = readVersion;
		state int checks = 0;

		state KeyRange last;
		state bool availabilityPassed = true;

		state Standalone<VectorRef<KeyRangeRef>> allRanges;
		if (self->granuleRanges.get().empty()) {
			if (BGV_DEBUG) {
				fmt::print("Waiting to get granule ranges for check\n");
			}
			state Future<Void> rangeFetcher = self->findGranules(cx, self);
			loop {
				wait(self->granuleRanges.onChange());
				// wait until entire keyspace has granules
				if (!self->granuleRanges.get().empty()) {
					bool haveAll = true;
					if (self->granuleRanges.get().front().begin != normalKeys.begin ||
					    self->granuleRanges.get().back().end != normalKeys.end) {
						haveAll = false;
					}
					for (int i = 0; haveAll && i < self->granuleRanges.get().size() - 1; i++) {
						if (self->granuleRanges.get()[i].end != self->granuleRanges.get()[i + 1].begin) {
							haveAll = false;
						}
					}
					if (haveAll) {
						break;
					}
				}
			}
			rangeFetcher.cancel();
			if (BGV_DEBUG) {
				fmt::print("Got granule ranges for check\n");
			}
		}
		allRanges = self->granuleRanges.get();
		for (auto& range : allRanges) {
			state KeyRange r = range;
			if (BGV_DEBUG) {
				fmt::print("Final availability check [{0} - {1}) @ {2}\n",
				           r.begin.printable(),
				           r.end.printable(),
				           readVersion);
			}

			try {
				loop {
					try {
						Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
						    wait(tr.readBlobGranules(r, 0, readVersion));
						ASSERT(chunks.size() > 0);
						last = chunks.back().keyRange;
						checks += chunks.size();

						break;
					} catch (Error& e) {
						// it's possible for blob granules to never get opened for the entire test due to fault
						// injection. If we get blob_granule_transaction_too_old, for the latest read version, the
						// granule still needs to open. Wait for that to happen at a higher read version.
						if (e.code() == error_code_blob_granule_transaction_too_old) {
							wait(delay(1.0));
							tr.reset();
							Version rv = wait(self->doGrv(&tr));
							readVersion = rv;
						} else {
							wait(tr.onError(e));
						}
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				if (e.code() == error_code_end_of_stream) {
					break;
				}
				if (BGV_DEBUG) {
					fmt::print("BG Verifier failed final availability check for [{0} - {1}) @ {2} with error {3}. Last "
					           "Success=[{4} - {5})\n",
					           r.begin.printable(),
					           r.end.printable(),
					           readVersion,
					           e.name(),
					           last.begin.printable(),
					           last.end.printable());
				}
				availabilityPassed = false;
				break;
			}
		}
		if (BGV_DEBUG && startReadVersion != readVersion) {
			fmt::print("Availability check updated read version from {0} to {1}\n", startReadVersion, readVersion);
		}
		state bool result =
		    availabilityPassed && self->mismatches == 0 && (checks > 0) && (self->timeTravelTooOld == 0);
		fmt::print("Blob Granule Verifier {0} {1}:\n", self->clientId, result ? "passed" : "failed");
		fmt::print("  {} successful final granule checks\n", checks);
		fmt::print("  {} failed final granule checks\n", availabilityPassed ? 0 : 1);
		fmt::print("  {} mismatches\n", self->mismatches);
		fmt::print("  {} time travel too old\n", self->timeTravelTooOld);
		fmt::print("  {} errors\n", self->errors);
		fmt::print("  {} initial reads\n", self->initialReads);
		fmt::print("  {} time travel reads\n", self->timeTravelReads);
		fmt::print("  {} rows\n", self->rowsRead);
		fmt::print("  {} bytes\n", self->bytesRead);
		fmt::print("  {} purges\n", self->purges);
		// FIXME: add above as details to trace event

		TraceEvent("BlobGranuleVerifierChecked").detail("Result", result);

		// For some reason simulation is still passing when this fails?.. so assert for now
		ASSERT(result);

		// FIXME: if doPurging was set, possibly do one last purge here, and verify it succeeds with no errors

		if (self->clientId == 0 && SERVER_KNOBS->BG_ENABLE_MERGING && self->clearAndMergeCheck) {
			CODE_PROBE(true, "BGV clearing database and awaiting merge");
			wait(clearAndAwaitMerge(cx, normalKeys));
		}

		return result;
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobGranuleVerifierWorkload> BlobGranuleVerifierWorkloadFactory("BlobGranuleVerifier");
