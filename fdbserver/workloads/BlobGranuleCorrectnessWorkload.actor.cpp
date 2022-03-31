/*
 * BlobGranuleCorrectnessWorkload.actor.cpp
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

#include <cmath>
#include <map>
#include <utility>
#include <vector>

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BGW_DEBUG true

struct WriteData {
	Version writeVersion;
	Version clearVersion;
	int32_t val;
	int16_t valLength;

	// start as MAX_VERSION while uncommitted/uncleared so that they're ignored by concurrent readers
	explicit WriteData(int32_t val, int16_t valLength)
	  : writeVersion(MAX_VERSION), clearVersion(MAX_VERSION), val(val), valLength(valLength) {}
};

struct KeyData {
	int nextClearIdx;
	std::vector<WriteData> writes;
};

static std::vector<int> targetValSizes = { 40, 100, 500 };

struct ThreadData : ReferenceCounted<ThreadData>, NonCopyable {
	// directory info
	int32_t directoryID;
	KeyRange directoryRange;

	// key + value gen data
	// in vector for efficient random selection
	std::vector<uint32_t> usedKeys;
	// by key for tracking data
	std::map<uint32_t, KeyData> keyData;

	std::deque<Version> writeVersions;

	// randomized parameters that can be different per directory
	int targetByteRate;
	bool nextKeySequential;
	int16_t targetValLength;
	double reuseKeyProb;
	int targetIDsPerKey;

	// communication between workers
	Promise<Void> firstWriteSuccessful;
	Version minSuccessfulReadVersion = MAX_VERSION;

	// stats
	int64_t errors = 0;
	int64_t mismatches = 0;
	int64_t reads = 0;
	int64_t timeTravelReads = 0;
	int64_t timeTravelTooOld = 0;
	int64_t rowsRead = 0;
	int64_t bytesRead = 0;
	int64_t rowsWritten = 0;
	int64_t bytesWritten = 0;

	ThreadData(uint32_t directoryID, int64_t targetByteRate)
	  : directoryID(directoryID), targetByteRate(targetByteRate) {
		directoryRange =
		    KeyRangeRef(StringRef(format("%08x", directoryID)), StringRef(format("%08x", directoryID + 1)));

		targetByteRate *= (0.5 + deterministicRandom()->random01());

		targetValLength = deterministicRandom()->randomChoice(targetValSizes);
		targetValLength *= (0.5 + deterministicRandom()->random01());

		nextKeySequential = deterministicRandom()->random01() < 0.5;
		reuseKeyProb = 0.1 + (deterministicRandom()->random01() * 0.8);
		targetIDsPerKey = 1 + deterministicRandom()->randomInt(1, 10);

		if (BGW_DEBUG) {
			fmt::print("Directory {0} initialized with the following parameters:\n", directoryID);
			fmt::print("  targetByteRate={0}\n", targetByteRate);
			fmt::print("  targetValLength={0}\n", targetValLength);
			fmt::print("  nextKeySequential={0}\n", nextKeySequential);
			fmt::print("  reuseKeyProb={0}\n", reuseKeyProb);
			fmt::print("  targetIDsPerKey={0}\n", targetIDsPerKey);
		}
	}

	// TODO could make keys variable length?
	Key getKey(uint32_t key, uint32_t id) { return StringRef(format("%08x/%08x/%08x", directoryID, key, id)); }
};

// For debugging mismatches on what data should be and why
// set mismatch to true, dir id and key id to the directory and key id that are wrong, and rv to read version that read
// the wrong value
#define DEBUG_MISMATCH false
#define DEBUG_DIR_ID 0
#define DEBUG_KEY_ID 0
#define DEBUG_RV invalidVersion

#define DEBUG_KEY_OP(dirId, keyId) BGW_DEBUG&& DEBUG_MISMATCH&& dirId == DEBUG_DIR_ID&& DEBUG_KEY_ID == keyId
#define DEBUG_READ_OP(dirId, rv) BGW_DEBUG&& DEBUG_MISMATCH&& dirId == DEBUG_DIR_ID&& rv == DEBUG_RV

/*
 * This is a stand-alone workload designed to validate blob granule correctness.
 * By enabling distinct ranges and writing to those parts of the key space, we can control what parts of the key space
 * are written to blob, and can validate that the granule data is correct at any desired version.
 */
struct BlobGranuleCorrectnessWorkload : TestWorkload {
	bool doSetup;
	double testDuration;

	// parameters global across all clients
	int64_t targetByteRate;

	std::vector<Reference<ThreadData>> directories;
	std::vector<Future<Void>> clients;
	DatabaseConfiguration config;
	Reference<BackupContainerFileSystem> bstore;

	BlobGranuleCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		doSetup = !clientId; // only do this on the "first" client
		testDuration = getOption(options, LiteralStringRef("testDuration"), 120.0);

		// randomize global test settings based on shared parameter to get similar workload across tests, but then vary
		// different parameters within those constraints
		int64_t randomness = sharedRandomNumber;

		// randomize between low and high directory count
		int64_t targetDirectories = 1 + (randomness % 8);
		randomness /= 8;

		int64_t targetMyDirectories =
		    (targetDirectories / clientCount) + ((targetDirectories % clientCount > clientId) ? 1 : 0);

		if (targetMyDirectories > 0) {
			int myDirectories = 1;
			if (targetMyDirectories > 1) {
				myDirectories = deterministicRandom()->randomInt(1, 2 * targetMyDirectories + 1);
			}

			// anywhere from 2 delta files per second to 1 delta file every 2 seconds, spread across all directories
			int denom = std::min(clientCount, (int)targetDirectories);
			targetByteRate = 2 * SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES / (1 + (randomness % 4)) / denom;
			randomness /= 4;

			// either do equal across all of my directories, or skewed
			bool skewed = myDirectories > 1 && deterministicRandom()->random01() < 0.4;
			int skewMultiplier;
			if (skewed) {
				// first directory has 1/2, second has 1/4, third has 1/8, etc...
				skewMultiplier = 2;
				targetByteRate /= 2;
			} else {
				skewMultiplier = 1;
				targetByteRate /= myDirectories;
			}
			for (int i = 0; i < myDirectories; i++) {
				// set up directory with its own randomness
				uint32_t dirId = i * clientCount + clientId;
				if (BGW_DEBUG) {
					fmt::print("Client {0}/{1} creating directory {2}\n", clientId, clientCount, dirId);
				}
				directories.push_back(makeReference<ThreadData>(dirId, targetByteRate));
				targetByteRate /= skewMultiplier;
			}
		}
	}

	ACTOR Future<Void> setUpBlobRange(Database cx, KeyRange range) {
		if (BGW_DEBUG) {
			fmt::print(
			    "Setting up blob granule range for [{0} - {1})\n", range.begin.printable(), range.end.printable());
		}
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->set(blobRangeChangeKey, deterministicRandom()->randomUniqueID().toString());
				wait(krmSetRange(tr, blobRangeKeys.begin, range, LiteralStringRef("1")));
				wait(tr->commit());
				if (BGW_DEBUG) {
					fmt::print("Successfully set up blob granule range for [{0} - {1})\n",
					           range.begin.printable(),
					           range.end.printable());
				}
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	std::string description() const override { return "BlobGranuleCorrectnessWorkload"; }
	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR Future<Void> _setup(Database cx, BlobGranuleCorrectnessWorkload* self) {
		if (self->doSetup) {
			// FIXME: run the actual FDBCLI command instead of copy/pasting its implementation
			wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));
		}

		if (self->directories.empty()) {
			return Void();
		}

		state int directoryIdx = 0;
		for (; directoryIdx < self->directories.size(); directoryIdx++) {
			// Set up the blob range first
			wait(self->setUpBlobRange(cx, self->directories[directoryIdx]->directoryRange));
		}

		if (BGW_DEBUG) {
			printf("Initializing Blob Granule Correctness s3 stuff\n");
		}
		try {
			if (g_network->isSimulated()) {
				if (BGW_DEBUG) {
					printf("Blob Granule Correctness constructing simulated backup container\n");
				}
				self->bstore = BackupContainerFileSystem::openContainerFS("file://fdbblob/", {}, {});
			} else {
				if (BGW_DEBUG) {
					printf("Blob Granule Correctness constructing backup container from %s\n",
					       SERVER_KNOBS->BG_URL.c_str());
				}
				self->bstore = BackupContainerFileSystem::openContainerFS(SERVER_KNOBS->BG_URL, {}, {});
				if (BGW_DEBUG) {
					printf("Blob Granule Correctness constructed backup container\n");
				}
			}
		} catch (Error& e) {
			if (BGW_DEBUG) {
				printf("Blob Granule Correctness got backup container init error %s\n", e.name());
			}
			throw e;
		}

		return Void();
	}

	// FIXME: typedef this pair type and/or chunk list
	ACTOR Future<std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>>> readFromBlob(
	    Database cx,
	    BlobGranuleCorrectnessWorkload* self,
	    KeyRange range,
	    Version beginVersion,
	    Version readVersion) {
		state RangeResult out;
		state Standalone<VectorRef<BlobGranuleChunkRef>> chunks;
		state Transaction tr(cx);

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
			RangeResult chunkRows = wait(readBlobGranule(chunk, range, beginVersion, readVersion, self->bstore));
			out.arena().dependsOn(chunkRows.arena());
			out.append(out.arena(), chunkRows.begin(), chunkRows.size());
		}
		return std::pair(out, chunks);
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

	ACTOR Future<Void> waitFirstSnapshot(BlobGranuleCorrectnessWorkload* self,
	                                     Database cx,
	                                     Reference<ThreadData> threadData,
	                                     bool doSetup) {
		// read entire keyspace at the start until granules for the entire thing are available
		loop {
			state Transaction tr(cx);
			try {
				Version rv = wait(self->doGrv(&tr));
				state Version readVersion = rv;
				std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob =
				    wait(self->readFromBlob(cx, self, threadData->directoryRange, 0, readVersion));
				fmt::print("Directory {0} got {1} RV {2}\n",
				           threadData->directoryID,
				           doSetup ? "initial" : "final",
				           readVersion);
				threadData->minSuccessfulReadVersion = readVersion;
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}
				if (e.code() != error_code_blob_granule_transaction_too_old) {
					wait(tr.onError(e));
				} else {
					wait(delay(1.0));
				}
			}
		}
	}

	void logMismatch(Reference<ThreadData> threadData,
	                 const Optional<Key>& lastMatching,
	                 const Optional<Key>& expectedKey,
	                 const Optional<Key>& blobKey,
	                 const Optional<Value>& expectedValue,
	                 const Optional<Value>& blobValue,
	                 uint32_t startKey,
	                 uint32_t endKey,
	                 Version beginVersion,
	                 Version readVersion,
	                 const std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>>& blob) {
		threadData->mismatches++;
		if (!BGW_DEBUG) {
			return;
		}

		TraceEvent ev(SevError, "BGMismatch");
		ev.detail("DirectoryID", format("%08x", threadData->directoryID))
		    .detail("RangeStart", format("%08x", startKey))
		    .detail("RangeEnd", format("%08x", endKey))
		    .detail("BeginVersion", beginVersion)
		    .detail("Version", readVersion);
		fmt::print("Found mismatch! Request for dir {0} [{1} - {2}) @ {3} - {4}\n",
		           format("%08x", threadData->directoryID),
		           format("%08x", startKey),
		           format("%08x", endKey),
		           beginVersion,
		           readVersion);
		if (lastMatching.present()) {
			fmt::print("    last correct: {}\n", lastMatching.get().printable());
		}
		if (expectedValue.present() || blobValue.present()) {
			// value mismatch
			ASSERT(blobKey.present());
			ASSERT(blobKey == expectedKey);
			fmt::print("  Value mismatch for {0}.\n    Expected={1}\n    Actual={2}\n",
			           blobKey.get().printable(),
			           expectedValue.get().printable(),
			           blobValue.get().printable());
		} else {
			// key mismatch
			fmt::print("    Expected Key: {0}\n", expectedKey.present() ? expectedKey.get().printable() : "<missing>");
			fmt::print("      Actual Key: {0}\n", blobKey.present() ? blobKey.get().printable() : "<missing>");
		}

		fmt::print("Chunks:\n");
		for (auto& chunk : blob.second) {
			fmt::print("[{0} - {1})\n", chunk.keyRange.begin.printable(), chunk.keyRange.end.printable());

			fmt::print("  SnapshotFile:\n    {}\n",
			           chunk.snapshotFile.present() ? chunk.snapshotFile.get().toString().c_str() : "<none>");
			fmt::print("  DeltaFiles:\n");
			for (auto& df : chunk.deltaFiles) {
				fmt::print("    {}\n", df.toString());
			}
			fmt::print("  Deltas: ({})", chunk.newDeltas.size());
			if (chunk.newDeltas.size() > 0) {
				fmt::print(" with version [{0} - {1}]",
				           chunk.newDeltas[0].version,
				           chunk.newDeltas[chunk.newDeltas.size() - 1].version);
			}
			fmt::print("  IncludedVersion: {}\n", chunk.includedVersion);
		}
		printf("\n");
	}

	Value genVal(uint32_t val, uint16_t valLen) {
		std::string v(valLen, 'x');
		auto valFormatted = format("%08x", val);
		ASSERT(valFormatted.size() <= v.size());

		for (int i = 0; i < valFormatted.size(); i++) {
			v[i] = valFormatted[i];
		}
		// copy into an arena
		// TODO do this in original arena? a bit more efficient that way
		Arena a;
		return Standalone<StringRef>(StringRef(a, v), a);
	}

	bool validateValue(const Value& v, uint32_t val, uint16_t valLen) {
		if (v.size() != valLen) {
			return false;
		}
		// check for correct value portion
		auto valFormatted = format("%08x", val);
		ASSERT(valFormatted.size() <= v.size());
		if (v.substr(0, valFormatted.size()) != valFormatted) {
			return false;
		}
		// check for corruption
		for (int i = valFormatted.size(); i < v.size(); i++) {
			if (v[i] != 'x') {
				return false;
			}
		}
		return true;
	}

	bool validateResult(Reference<ThreadData> threadData,
	                    std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob,
	                    int startKeyInclusive,
	                    int endKeyExclusive,
	                    Version beginVersion,
	                    Version readVersion) {
		auto checkIt = threadData->keyData.lower_bound(startKeyInclusive);
		if (checkIt != threadData->keyData.end() && checkIt->first < startKeyInclusive) {
			checkIt++;
		}
		int resultIdx = 0;
		Optional<Key> lastMatching;
		if (DEBUG_READ_OP(threadData->directoryID, readVersion)) {
			fmt::print("DBG READ: [{0} - {1}) @ {2}\n",
			           format("%08x", startKeyInclusive),
			           format("%08x", endKeyExclusive),
			           readVersion);
		}

		// because each chunk could be separately collapsed or not if we set beginVersion, we have to track it by chunk
		KeyRangeMap<Version> beginVersionByChunk;
		beginVersionByChunk.insert(normalKeys, 0);
		int beginCollapsed = 0;
		int beginNotCollapsed = 0;
		for (auto& chunk : blob.second) {
			if (!chunk.snapshotFile.present()) {
				ASSERT(beginVersion > 0);
				ASSERT(chunk.snapshotVersion == invalidVersion);
				beginCollapsed++;
				beginVersionByChunk.insert(chunk.keyRange, beginVersion);
			} else {
				ASSERT(chunk.snapshotVersion != invalidVersion);
				if (beginVersion > 0) {
					beginNotCollapsed++;
				}
			}
		}
		TEST(beginCollapsed > 0); // BGCorrectness got collapsed request with beginVersion > 0
		TEST(beginNotCollapsed > 0); // BGCorrectness got un-collapsed request with beginVersion > 0
		TEST(beginCollapsed > 0 &&
		     beginNotCollapsed > 0); // BGCorrectness got both collapsed and uncollapsed in the same request!

		while (checkIt != threadData->keyData.end() && checkIt->first < endKeyExclusive) {
			uint32_t key = checkIt->first;
			if (DEBUG_READ_OP(threadData->directoryID, readVersion)) {
				fmt::print("DBG READ:   Key {0}\n", format("%08x", key));
			}

			// TODO could binary search this to find clearVersion if it gets long
			int idIdx = 0;
			for (; idIdx < checkIt->second.writes.size() && checkIt->second.writes[idIdx].clearVersion <= readVersion;
			     idIdx++) {
				// iterate until we find the oldest tag that should have not been cleared
				/*if (DEBUG_READ_OP(threadData->directoryID, readVersion)) {
				    fmt::print(
				        "DBG READ:     Skip ID {0} cleared @ {1}\n", idIdx, checkIt->second.writes[idIdx].clearVersion);
				}*/
			}
			for (; idIdx < checkIt->second.writes.size() && checkIt->second.writes[idIdx].writeVersion <= readVersion;
			     idIdx++) {
				Key nextKeyShouldBe = threadData->getKey(key, idIdx);
				Version keyBeginVersion = beginVersionByChunk.rangeContaining(nextKeyShouldBe).cvalue();
				if (keyBeginVersion > checkIt->second.writes[idIdx].writeVersion) {
					if (DEBUG_READ_OP(threadData->directoryID, readVersion)) {
						fmt::print("DBG READ:     Skip ID {0} written @ {1} < beginVersion {2}\n",
						           idIdx,
						           checkIt->second.writes[idIdx].clearVersion,
						           keyBeginVersion);
					}
					continue;
				}
				if (DEBUG_READ_OP(threadData->directoryID, readVersion)) {
					fmt::print("DBG READ:     Checking ID {0} ({1}) written @ {2}\n",
					           format("%08x", idIdx),
					           idIdx,
					           checkIt->second.writes[idIdx].writeVersion);
				}
				if (resultIdx >= blob.first.size()) {
					// missing at end!!
					logMismatch(threadData,
					            lastMatching,
					            nextKeyShouldBe,
					            Optional<Key>(),
					            Optional<Value>(),
					            Optional<Value>(),
					            startKeyInclusive,
					            endKeyExclusive,
					            beginVersion,
					            readVersion,
					            blob);
					return false;
				}

				if (nextKeyShouldBe != blob.first[resultIdx].key) {
					// key mismatch!
					if (DEBUG_READ_OP(threadData->directoryID, readVersion)) {
						printf("key mismatch!\n");
					}
					logMismatch(threadData,
					            lastMatching,
					            nextKeyShouldBe,
					            blob.first[resultIdx].key,
					            Optional<Value>(),
					            Optional<Value>(),
					            startKeyInclusive,
					            endKeyExclusive,
					            beginVersion,
					            readVersion,
					            blob);
					return false;
				} else if (!validateValue(blob.first[resultIdx].value,
				                          checkIt->second.writes[idIdx].val,
				                          checkIt->second.writes[idIdx].valLength)) {
					logMismatch(threadData,
					            lastMatching,
					            nextKeyShouldBe,
					            blob.first[resultIdx].key,
					            genVal(checkIt->second.writes[idIdx].val, checkIt->second.writes[idIdx].valLength),
					            blob.first[resultIdx].value,
					            startKeyInclusive,
					            endKeyExclusive,
					            beginVersion,
					            readVersion,
					            blob);
					return false;
					// value mismatch for same key
				} else {
					lastMatching = nextKeyShouldBe;
				}
				resultIdx++;
			}
			checkIt++;
		}

		if (resultIdx < blob.first.size()) {
			// blob has extra stuff!!
			logMismatch(threadData,
			            lastMatching,
			            Optional<Key>(),
			            blob.first[resultIdx].key,
			            Optional<Value>(),
			            Optional<Value>(),
			            startKeyInclusive,
			            endKeyExclusive,
			            beginVersion,
			            readVersion,
			            blob);
			return false;
		}

		return true;
	}

	ACTOR Future<Void> readWorker(BlobGranuleCorrectnessWorkload* self,
	                              Future<Void> firstSnapshot,
	                              Database cx,
	                              Reference<ThreadData> threadData) {
		state double last = now();
		state double targetBytesReadPerQuery =
		    SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES * 2.0 / deterministicRandom()->randomInt(1, 11);

		// read at higher read rate than write rate to validate data
		state double targetReadBytesPerSec = threadData->targetByteRate * 4;
		ASSERT(targetReadBytesPerSec > 0);

		state Version beginVersion;
		state Version readVersion;

		TraceEvent("BlobGranuleCorrectnessReaderStart").log();
		if (BGW_DEBUG) {
			printf("BGW read thread starting\n");
		}

		// wait for data to read
		wait(firstSnapshot);
		wait(threadData->firstWriteSuccessful.getFuture());

		TraceEvent("BlobGranuleCorrectnessReaderReady").log();
		if (BGW_DEBUG) {
			printf("BGW read thread ready\n");
		}

		loop {
			try {
				// Do 1 read

				// pick key range by doing random start key, and then picking the end key based on that
				int startKeyIdx = deterministicRandom()->randomInt(0, threadData->usedKeys.size());
				state uint32_t startKey = threadData->usedKeys[startKeyIdx];
				auto endKeyIt = threadData->keyData.find(startKey);
				ASSERT(endKeyIt != threadData->keyData.end());

				int targetQueryBytes = (deterministicRandom()->randomInt(1, 20) * targetBytesReadPerQuery) / 10;
				int estimatedQueryBytes = 0;
				for (int i = 0; estimatedQueryBytes < targetQueryBytes && endKeyIt != threadData->keyData.end();
				     i++, endKeyIt++) {
					// iterate forward until end or target keys have passed
					estimatedQueryBytes += (1 + endKeyIt->second.writes.size() - endKeyIt->second.nextClearIdx) *
					                       threadData->targetValLength;
				}

				state uint32_t endKey;
				if (endKeyIt == threadData->keyData.end()) {
					endKey = std::numeric_limits<uint32_t>::max();
				} else {
					endKey = endKeyIt->first;
				}

				state KeyRange range = KeyRangeRef(threadData->getKey(startKey, 0), threadData->getKey(endKey, 0));

				// pick read version
				ASSERT(threadData->writeVersions.back() >= threadData->minSuccessfulReadVersion);
				size_t readVersionIdx;
				// randomly choose up to date vs time travel read
				if (deterministicRandom()->random01() < 0.5) {
					threadData->reads++;
					readVersionIdx = threadData->writeVersions.size() - 1;
					readVersion = threadData->writeVersions.back();
				} else {
					threadData->timeTravelReads++;
					size_t startIdx = 0;
					loop {
						readVersionIdx = deterministicRandom()->randomInt(startIdx, threadData->writeVersions.size());
						readVersion = threadData->writeVersions[readVersionIdx];
						if (readVersion >= threadData->minSuccessfulReadVersion) {
							break;
						} else {
							startIdx = readVersionIdx + 1;
						}
					}
				}

				// randomly choose begin version or not
				beginVersion = 0;
				if (deterministicRandom()->random01() < 0.5) {
					int startIdx = 0;
					int endIdxExclusive = readVersionIdx + 1;
					// Choose skewed towards later versions. It's ok if beginVersion isn't readable though because it
					// will collapse
					size_t beginVersionIdx = (size_t)std::sqrt(
					    deterministicRandom()->randomInt(startIdx * startIdx, endIdxExclusive * endIdxExclusive));
					beginVersion = threadData->writeVersions[beginVersionIdx];
				}

				std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob =
				    wait(self->readFromBlob(cx, self, range, beginVersion, readVersion));
				self->validateResult(threadData, blob, startKey, endKey, beginVersion, readVersion);

				int resultBytes = blob.first.expectedSize();
				threadData->rowsRead += blob.first.size();
				threadData->bytesRead += resultBytes;

				wait(poisson(&last, (resultBytes + 1) / targetReadBytesPerSec));
			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw;
				}
				if (e.code() == error_code_blob_granule_transaction_too_old) {
					threadData->timeTravelTooOld++;
				} else {
					threadData->errors++;
					if (BGW_DEBUG) {
						printf("BGWorkload got unexpected error %s\n", e.name());
					}
				}
			}
		}
	}

	ACTOR Future<Void> writeWorker(BlobGranuleCorrectnessWorkload* self,
	                               Future<Void> firstSnapshot,
	                               Database cx,
	                               Reference<ThreadData> threadData) {

		state double last = now();
		state int keysPerQuery = 100;
		// state int targetBytesPerQuery = threadData->targetValLength * keysPerQuery;
		// state double targetTps = (1.0 * threadData->targetByteRate) / targetBytesPerQuery;
		state uint32_t nextVal = 0;

		TraceEvent("BlobGranuleCorrectnessWriterStart").log();

		wait(firstSnapshot);

		TraceEvent("BlobGranuleCorrectnessWriterReady").log();

		loop {
			state Transaction tr(cx);

			// pick rows to write and clear, generate values for writes
			state std::vector<std::tuple<uint32_t, uint32_t, uint32_t, uint16_t>> keyAndIdToWrite;
			state std::vector<std::pair<uint32_t, uint32_t>> keyAndIdToClear;

			state int queryKeys =
			    keysPerQuery * (0.1 + deterministicRandom()->random01() * 1.8); // 10% to 190% of target keys per query
			for (int i = 0; i < queryKeys; i++) {
				uint32_t key;
				if (threadData->keyData.empty() || deterministicRandom()->random01() > threadData->reuseKeyProb) {
					// new key
					if (threadData->nextKeySequential) {
						key = threadData->usedKeys.size();
					} else {
						key = std::numeric_limits<uint32_t>::max();
						while (key == std::numeric_limits<uint32_t>::max() ||
						       threadData->keyData.find(key) != threadData->keyData.end()) {
							key = deterministicRandom()->randomUInt32();
						}
					}

					// add new key to data structures
					threadData->usedKeys.push_back(key);
					threadData->keyData.insert({ key, KeyData() });
				} else {
					int keyIdx = deterministicRandom()->randomInt(0, threadData->usedKeys.size());
					key = threadData->usedKeys[keyIdx];
				}

				auto keyIt = threadData->keyData.find(key);
				ASSERT(keyIt != threadData->keyData.end());

				int unclearedIds = keyIt->second.writes.size() - keyIt->second.nextClearIdx;
				// if we are at targetIDs, 50% chance of adding one or clearing. If we are closer to 0, higher chance of
				// adding one, if we are closer to 2x target IDs, higher chance of clearing one
				double probAddId = (threadData->targetIDsPerKey * 2.0 - unclearedIds) / threadData->targetIDsPerKey;
				if (deterministicRandom()->random01() < probAddId ||
				    keyIt->second.nextClearIdx == keyIt->second.writes.size()) {
					int32_t val = nextVal++;
					int16_t valLen = (0.5 + deterministicRandom()->random01()) * threadData->targetValLength;
					if (valLen < 10) {
						valLen = 10;
					}

					uint32_t nextId = keyIt->second.writes.size();
					keyIt->second.writes.push_back(WriteData(val, valLen));

					keyAndIdToWrite.push_back(std::tuple(key, nextId, val, valLen));
				} else {
					uint32_t idToClear = keyIt->second.nextClearIdx++;
					keyAndIdToClear.push_back(std::pair(key, idToClear));
				}
			}

			state int64_t txnBytes;
			loop {
				try {
					// write rows in txn
					for (auto& it : keyAndIdToWrite) {
						Value v = self->genVal(std::get<2>(it), std::get<3>(it));
						tr.set(threadData->getKey(std::get<0>(it), std::get<1>(it)), v);
					}
					for (auto& it : keyAndIdToClear) {
						tr.clear(singleKeyRange(threadData->getKey(it.first, it.second)));
					}
					txnBytes = tr.getSize();
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			Version commitVersion = tr.getCommittedVersion();

			// once txn is committed, update write map

			for (auto& it : keyAndIdToWrite) {
				uint32_t key = std::get<0>(it);
				uint32_t id = std::get<1>(it);
				auto keyIt = threadData->keyData.find(key);
				ASSERT(keyIt != threadData->keyData.end());

				keyIt->second.writes[id].writeVersion = commitVersion;
				if (DEBUG_KEY_OP(threadData->directoryID, key)) {
					fmt::print("DBG: {0} WRITE {1} = {2}:{3}\n",
					           commitVersion,
					           format("%08x/%08x/%08x", threadData->directoryID, key, id),
					           std::get<2>(it),
					           std::get<3>(it));
				}
			}

			for (auto& it : keyAndIdToClear) {
				auto keyIt = threadData->keyData.find(it.first);
				ASSERT(keyIt != threadData->keyData.end());
				keyIt->second.writes[it.second].clearVersion = commitVersion;
				if (DEBUG_KEY_OP(threadData->directoryID, it.first)) {
					fmt::print("DBG: {0} CLEAR {1}\n",
					           commitVersion,
					           format("%08x/%08x/%08x", threadData->directoryID, it.first, it.second));
				}
			}

			threadData->writeVersions.push_back(commitVersion);

			if (threadData->firstWriteSuccessful.canBeSet()) {
				threadData->firstWriteSuccessful.send(Void());
			}

			threadData->rowsWritten += queryKeys;
			threadData->bytesWritten += txnBytes;

			// wait
			wait(poisson(&last, (txnBytes + 1.0) / threadData->targetByteRate));
		}
	}

	Future<Void> start(Database const& cx) override {
		clients.reserve(3 * directories.size());
		for (auto& it : directories) {
			// Wait for blob worker to initialize snapshot before starting test for that range
			Future<Void> start = waitFirstSnapshot(this, cx, it, true);
			clients.push_back(timeout(writeWorker(this, start, cx, it), testDuration, Void()));
			clients.push_back(timeout(readWorker(this, start, cx, it), testDuration, Void()));
		}
		return delay(testDuration);
	}

	ACTOR Future<bool> checkDirectory(Database cx,
	                                  BlobGranuleCorrectnessWorkload* self,
	                                  Reference<ThreadData> threadData) {

		state bool result = true;
		state int finalRowsValidated;
		if (threadData->writeVersions.empty()) {
			// never had a successful write during the test, likely due to many chaos events. Just wait for granules to
			// become available and call that a pass, since writer is stopped and will never guarantee anything is
			// written
			if (BGW_DEBUG) {
				fmt::print("Directory {0} doing final availability check\n", threadData->directoryID);
			}
			wait(self->waitFirstSnapshot(self, cx, threadData, false));
		} else {
			// otherwise, read at last write version and ensure everything becomes available and matches
			// it's possible that waitFirstSnapshot finished but then writer never wrote anything before test timed out
			state Version readVersion = threadData->writeVersions.back();
			if (BGW_DEBUG) {
				fmt::print("Directory {0} doing final data check @ {1}\n", threadData->directoryID, readVersion);
			}
			std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob =
			    wait(self->readFromBlob(cx, self, threadData->directoryRange, 0, readVersion));
			result = self->validateResult(threadData, blob, 0, std::numeric_limits<uint32_t>::max(), 0, readVersion);
			finalRowsValidated = blob.first.size();

			// then if we are still good, do another check at a higher version (not checking data) to ensure availabiity
			// of empty versions
			if (result) {
				if (BGW_DEBUG) {
					fmt::print("Directory {0} doing final availability check after data check\n",
					           threadData->directoryID);
				}
				wait(self->waitFirstSnapshot(self, cx, threadData, false));
			}
		}

		bool initialCheck = result;
		result &= threadData->mismatches == 0 && (threadData->timeTravelTooOld == 0);

		fmt::print("Blob Granule Workload Directory {0} {1}:\n", threadData->directoryID, result ? "passed" : "failed");
		fmt::print("  Final granule check {0}successful\n", initialCheck ? "" : "un");
		fmt::print("  {} Rows read in final check\n", finalRowsValidated);
		fmt::print("  {} mismatches\n", threadData->mismatches);
		fmt::print("  {} time travel too old\n", threadData->timeTravelTooOld);
		fmt::print("  {} errors\n", threadData->errors);
		fmt::print("  {} rows written\n", threadData->rowsWritten);
		fmt::print("  {} bytes written\n", threadData->bytesWritten);
		fmt::print("  {} unique keys\n", threadData->usedKeys.size());
		fmt::print("  {} real-time reads\n", threadData->reads);
		fmt::print("  {} time travel reads\n", threadData->timeTravelReads);
		fmt::print("  {} rows read\n", threadData->rowsRead);
		fmt::print("  {} bytes read\n", threadData->bytesRead);
		// FIXME: add above as details to trace event

		TraceEvent("BlobGranuleWorkloadChecked").detail("Directory", threadData->directoryID).detail("Result", result);

		// For some reason simulation is still passing when this fails?.. so assert for now
		ASSERT(result);

		return result;
	}

	ACTOR Future<bool> _check(Database cx, BlobGranuleCorrectnessWorkload* self) {
		// check error counts, and do an availability check at the end
		state std::vector<Future<bool>> results;
		for (auto& it : self->directories) {
			results.push_back(self->checkDirectory(cx, self, it));
		}
		state bool allSuccessful = true;
		for (auto& f : results) {
			bool dirSuccess = wait(f);
			allSuccessful &= dirSuccess;
		}
		return allSuccessful;
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobGranuleCorrectnessWorkload> BlobGranuleCorrectnessWorkloadFactory("BlobGranuleCorrectnessWorkload");
