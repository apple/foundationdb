/*
 * BlobGranuleVerifier.actor.cpp
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

#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * This workload is designed to verify the correctness of the blob data produced by the blob workers.
 * As a read-only validation workload, it can piggyback off of other write or read/write workloads.
 * To verify the data outside FDB's 5 second MVCC window, it tests time travel reads by doing an initial comparison at
 * the latest read version, and then waiting a period of time to re-read the data from blob.
 * To catch availability issues with the blob worker, it does a request to each granule at the end of the test.
 */
struct BlobGranuleVerifierWorkload : TestWorkload {
	// TODO add delay on start so it can start with data

	bool doSetup;
	double minDelay;
	double maxDelay;
	double testDuration;
	double timeTravelLimit;
	uint64_t timeTravelBufferSize;
	int threads;
	int64_t errors = 0;
	int64_t mismatches = 0;
	int64_t initialReads = 0;
	int64_t timeTravelReads = 0;
	int64_t rowsRead = 0;
	int64_t bytesRead = 0;
	vector<Future<Void>> clients;

	Reference<BackupContainerFileSystem> bstore;
	AsyncVar<std::vector<KeyRange>> granuleRanges;

	BlobGranuleVerifierWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		doSetup = !clientId; // only do this on the "first" client
		// FIXME: don't do the delay in setup, as that delays the start of all workloads
		minDelay = getOption(options, LiteralStringRef("minDelay"), 0.0);
		maxDelay = getOption(options, LiteralStringRef("maxDelay"), 0.0);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 120.0);
		timeTravelLimit = getOption(options, LiteralStringRef("timeTravelLimit"), testDuration);
		timeTravelBufferSize = getOption(options, LiteralStringRef("timeTravelBufferSize"), 100000000);
		threads = getOption(options, LiteralStringRef("threads"), 1);
		ASSERT(threads >= 1);

		printf("Initializing Blob Granule Verifier s3 stuff\n");
		try {
			if (g_network->isSimulated()) {
				printf("Blob Granule Verifier constructing simulated backup container\n");
				bstore = BackupContainerFileSystem::openContainerFS("file://fdbblob/");
			} else {
				printf("Blob Granule Verifier constructing backup container from %s\n", SERVER_KNOBS->BG_URL.c_str());
				bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL);
				printf("Blob Granule Verifier constructed backup container\n");
			}
		} catch (Error& e) {
			printf("BW got backup container init error %s\n", e.name());
			throw e;
		}
	}

	// FIXME: run the actual FDBCLI command instead of copy/pasting its implementation
	// Sets the whole user keyspace to be blobified
	ACTOR Future<Void> setUpBlobRange(Database cx, Future<Void> waitForStart) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		wait(waitForStart);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->set(blobRangeChangeKey, deterministicRandom()->randomUniqueID().toString());
				wait(krmSetRange(tr, blobRangeKeys.begin, KeyRange(normalKeys), LiteralStringRef("1")));
				wait(tr->commit());
				printf("Successfully set up blob granule range for normalKeys\n");
				TraceEvent("BlobGranuleVerifierSetup");
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	std::string description() const override { return "BlobGranuleVerifier"; }
	Future<Void> setup(Database const& cx) override {
		if (doSetup) {
			double initialDelay = deterministicRandom()->random01() * (maxDelay - minDelay) + minDelay;
			printf("BGW setup initial delay of %.3f\n", initialDelay);
			return setUpBlobRange(cx, delay(initialDelay));
		}
		return delay(0);
	}

	ACTOR Future<Void> findGranules(Database cx, BlobGranuleVerifierWorkload* self) {
		// updates the current set of granules in the database, but on a delay, so there can be some mismatch if ranges
		// change
		// printf("BGV find granules starting\n");
		loop {
			state std::vector<KeyRange> allGranules;
			state Transaction tr(cx);
			state PromiseStream<KeyRange> stream;
			state Future<Void> reader = cx->getBlobGranuleRangesStream(stream, normalKeys);
			loop {
				try {
					KeyRange r = waitNext(stream.getFuture());
					allGranules.push_back(r);
				} catch (Error& e) {
					if (e.code() == error_code_end_of_stream) {
						break;
					}
					throw e;
				}
			}
			wait(reader);
			// printf("BG find granules found %d granules\n", allGranules.size());
			self->granuleRanges.set(allGranules);

			wait(delay(deterministicRandom()->random01() * 10.0));
		}
	}

	// assumes we can read the whole range in one transaction at a single version
	ACTOR Future<std::pair<RangeResult, Version>> readFromFDB(Database cx, KeyRange range) {
		state Version v;
		state RangeResult out;
		state Transaction tr(cx);
		state KeyRange currentRange = range;
		loop {
			try {
				RangeResult r = wait(tr.getRange(currentRange, CLIENT_KNOBS->TOO_MANY));
				out.arena().dependsOn(r.arena());
				out.append(out.arena(), r.begin(), r.size());
				if (r.more) {
					currentRange = KeyRangeRef(keyAfter(r.back().key), currentRange.end);
				} else {
					Version _v = wait(tr.getReadVersion());
					v = _v;
					break;
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return std::pair(out, v);
	}

	ACTOR Future<RangeResult> readFromBlob(Database cx,
	                                       BlobGranuleVerifierWorkload* self,
	                                       KeyRange range,
	                                       Version version) {
		state RangeResult out;
		state PromiseStream<Standalone<BlobGranuleChunkRef>> chunkStream;
		state Future<Void> requester = cx->readBlobGranulesStream(chunkStream, range, 0, version);
		loop {
			try {
				Standalone<BlobGranuleChunkRef> nextChunk = waitNext(chunkStream.getFuture());
				out.arena().dependsOn(
				    nextChunk.arena()); // TODO this wastes extra memory but we'll se   e if it fixes segfault
				RangeResult chunkRows = wait(readBlobGranule(nextChunk, range, version, self->bstore));
				out.arena().dependsOn(chunkRows.arena());
				out.append(out.arena(), chunkRows.begin(), chunkRows.size());
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream) {
					break;
				}
				throw e;
			}
		}
		return out;
	}

	bool compareResult(RangeResult fdb, RangeResult blob, KeyRange range, Version v, bool initialRequest) {
		bool correct = fdb == blob;
		if (!correct) {
			mismatches++;
			TraceEvent ev(SevError, "GranuleMismatch");
			ev.detail("RangeStart", range.begin)
			    .detail("RangeEnd", range.end)
			    .detail("Version", v)
			    .detail("RequestType", initialRequest ? "RealTime" : "TimeTravel");
			// TODO debugging details!
		}
		return correct;
	}

	struct OldRead {
		KeyRange range;
		Version v;
		RangeResult oldResult;

		OldRead() {}
		OldRead(KeyRange range, Version v, RangeResult oldResult) : range(range), v(v), oldResult(oldResult) {}
		// OldRead(const OldRead& other) : range(other.range), v(other.v), oldResult(other.oldResult) {}
	};

	ACTOR Future<Void> verifyGranules(Database cx, BlobGranuleVerifierWorkload* self) {
		// TODO add time travel + verification
		state double last = now();
		state double endTime = last + self->testDuration;
		state std::map<double, OldRead> timeTravelChecks;
		state int64_t timeTravelChecksMemory = 0;

		TraceEvent("BlobGranuleVerifierStart");
		printf("BGV thread starting\n");

		// wait for first set of ranges to be loaded
		wait(self->granuleRanges.onChange());

		printf("BGV got ranges\n");

		loop {
			try {
				state double currentTime = now();
				state std::map<double, OldRead>::iterator timeTravelIt = timeTravelChecks.begin();
				while (timeTravelIt != timeTravelChecks.end() && currentTime >= timeTravelIt->first) {
					state OldRead oldRead = timeTravelIt->second;
					RangeResult reReadResult = wait(self->readFromBlob(cx, self, oldRead.range, oldRead.v));
					self->compareResult(oldRead.oldResult, reReadResult, oldRead.range, oldRead.v, false);

					timeTravelChecksMemory -= oldRead.oldResult.expectedSize();
					timeTravelIt = timeTravelChecks.erase(timeTravelIt);
					self->timeTravelReads++;
				}

				// pick a random range
				int rIndex = deterministicRandom()->randomInt(0, self->granuleRanges.get().size());
				state KeyRange range = self->granuleRanges.get()[rIndex];

				state std::pair<RangeResult, Version> fdb = wait(self->readFromFDB(cx, range));
				RangeResult blob = wait(self->readFromBlob(cx, self, range, fdb.second));
				if (self->compareResult(fdb.first, blob, range, fdb.second, true)) {
					// TODO: bias for immediately re-reading to catch rollback cases
					double reReadTime = currentTime + deterministicRandom()->random01() * self->timeTravelLimit;
					int memory = fdb.first.expectedSize();
					if (reReadTime <= endTime &&
					    timeTravelChecksMemory + memory <= (self->timeTravelBufferSize / self->threads)) {
						timeTravelChecks[reReadTime] = OldRead(range, fdb.second, fdb.first);
						timeTravelChecksMemory += memory;
					}
				}
				self->rowsRead += fdb.first.size();
				self->bytesRead += fdb.first.expectedSize();
				self->initialReads++;

			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw;
				}
				if (e.code() != error_code_transaction_too_old && e.code() != error_code_wrong_shard_server) {
					printf("BGVerifier got unexpected error %s\n", e.name());
				}
				self->errors++;
			}
			// TODO increase frequency a lot!! just for initial testing
			wait(poisson(&last, 5.0));
			// wait(poisson(&last, 0.1));
		}
	}

	Future<Void> start(Database const& cx) override {
		clients.reserve(threads + 1);
		clients.push_back(timeout(findGranules(cx, this), testDuration, Void()));
		for (int i = 0; i < threads; i++) {
			clients.push_back(
			    timeout(reportErrors(verifyGranules(cx, this), "BlobGranuleVerifier"), testDuration, Void()));
		}

		printf("BGF start launched\n");
		return delay(testDuration);
	}

	ACTOR Future<bool> _check(Database cx, BlobGranuleVerifierWorkload* self) {
		// check error counts, and do an availability check at the end

		Transaction tr(cx);
		state Version readVersion = wait(tr.getReadVersion());
		state int checks = 0;

		state vector<KeyRange> allRanges = self->granuleRanges.get();
		for (auto& range : allRanges) {
			state KeyRange r = range;
			state PromiseStream<Standalone<BlobGranuleChunkRef>> chunkStream;
			printf("Final availability check [%s - %s)\n", r.begin.printable().c_str(), r.end.printable().c_str());
			state Future<Void> requester = cx->readBlobGranulesStream(chunkStream, r, 0, readVersion);
			loop {
				try {
					// just make sure granule returns a non-error response, to ensure the range wasn't lost and the
					// workers are all caught up. Kind of like a quiet database check, just for the blob workers
					Standalone<BlobGranuleChunkRef> nextChunk = waitNext(chunkStream.getFuture());
					checks++;
				} catch (Error& e) {
					if (e.code() == error_code_end_of_stream) {
						break;
					}
					printf("BG Verifier failed final availability check for [%s - %s) @ %lld with error %s\n",
					       r.begin.printable().c_str(),
					       r.end.printable().c_str(),
					       readVersion,
					       e.name());
					throw e;
				}
			}
		}
		printf("Blob Granule Verifier finished with:\n");
		printf("  %lld mismatches\n", self->mismatches);
		printf("  %lld errors\n", self->errors);
		printf("  %lld initial reads\n", self->initialReads);
		printf("  %lld time travel reads\n", self->timeTravelReads);
		printf("  %lld rows\n", self->rowsRead);
		printf("  %lld bytes\n", self->bytesRead);
		printf("  %d final granule checks\n", checks);
		TraceEvent("BlobGranuleVerifierChecked");
		return self->mismatches == 0 && checks > 0;
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobGranuleVerifierWorkload> BlobGranuleVerifierWorkloadFactory("BlobGranuleVerifier");