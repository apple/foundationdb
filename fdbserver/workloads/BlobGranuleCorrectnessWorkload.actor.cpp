/*
 * BlobGranuleCorrectnessWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include <string>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/BlobGranuleValidation.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BGW_DEBUG true
#define BGW_TUPLE_KEY_SIZE 2

struct WriteData {
	Version writeVersion;
	Version clearVersion;
	int32_t val;
	int16_t valLength;

	// start as MAX_VERSION while uncommitted/uncleared so that they're ignored by concurrent readers
	explicit WriteData(int32_t val, int16_t valLength)
	  : writeVersion(MAX_VERSION), clearVersion(MAX_VERSION), val(val), valLength(valLength) {}

	// loading existing write data from the database
	explicit WriteData(Version writeVersion, Version clearVersion, int32_t val, int16_t valLength)
	  : writeVersion(writeVersion), clearVersion(clearVersion), val(val), valLength(valLength) {}
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
	TenantName tenantName;
	Reference<Tenant> tenant;
	TenantMapEntry tenantEntry;
	Reference<BlobConnectionProvider> bstore;

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
	uint32_t nextSeqKey = 0;

	// communication between workers
	Promise<Void> firstWriteSuccessful;
	Version minSuccessfulReadVersion = MAX_VERSION;

	Future<Void> summaryClient;
	Future<Void> forceFlushingClient;
	Promise<Void> triggerSummaryComplete;

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
		tenantName = StringRef(std::to_string(directoryID));

		targetByteRate *= (0.5 + deterministicRandom()->random01());

		targetValLength = deterministicRandom()->randomChoice(targetValSizes);
		targetValLength *= (0.5 + deterministicRandom()->random01());

		nextKeySequential = deterministicRandom()->random01() < 0.5;
		reuseKeyProb = 0.1 + (deterministicRandom()->random01() * 0.8);
		targetIDsPerKey = 1 + deterministicRandom()->randomInt(10, 100);

		if (BGW_DEBUG) {
			fmt::print("Directory {0} initialized with the following parameters:\n", directoryID);
			fmt::print("  targetByteRate={0}\n", targetByteRate);
			fmt::print("  targetValLength={0}\n", targetValLength);
			fmt::print("  nextKeySequential={0}\n", nextKeySequential);
			fmt::print("  reuseKeyProb={0}\n", reuseKeyProb);
			fmt::print("  targetIDsPerKey={0}\n", targetIDsPerKey);
		}
	}

	Future<Void> openTenant(Database const& cx) {
		tenant = makeReference<Tenant>(cx, tenantName);
		return tenant->ready();
	}

	// randomly reopen tenant and do not wait for it to be ready, to test races
	void maybeReopenTenant(Database const& cx) {
		if (BUGGIFY_WITH_PROB(0.01)) {
			openTenant(cx);
		}
	}

	// TODO could make keys variable length?
	Key getKey(uint32_t key, uint32_t id) {
		std::stringstream ss;
		ss << std::setw(32) << std::setfill('0') << id;
		if (g_network->isSimulated() && g_simulator->dataAtRestPlaintextMarker.present()) {
			ss << g_simulator->dataAtRestPlaintextMarker.get();
		}

		Standalone<StringRef> str(ss.str());
		Tuple::UserTypeStr udt(0x41, str);
		return Tuple::makeTuple((int64_t)key, udt).pack();
	}

	void validateGranuleBoundary(Key k, Key e, Key lastKey) {
		if (k == allKeys.begin || k == allKeys.end) {
			return;
		}

		// Fully formed tuples are inserted. The expectation is boundaries should be a
		// sub-tuple of the inserted key.
		Tuple t = Tuple::unpackUserType(k, true);
		if (SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET) {
			Tuple t2;
			try {
				t2 = Tuple::unpackUserType(lastKey);
			} catch (Error& e) {
				// Ignore being unable to parse lastKey as it may be a dummy key.
			}

			if (t2.size() > 0 && t.getInt(0) != t2.getInt(0)) {
				if (t.size() > BGW_TUPLE_KEY_SIZE - SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET) {
					fmt::print("Tenant: {0}, K={1}, E={2}, LK={3}. {4} != {5}\n",
					           tenantEntry.prefix.printable(),
					           k.printable(),
					           e.printable(),
					           lastKey.printable(),
					           t.getInt(0),
					           t2.getInt(0));
				}
				ASSERT(t.size() <= BGW_TUPLE_KEY_SIZE - SERVER_KNOBS->BG_KEY_TUPLE_TRUNCATE_OFFSET);
			}
		}
	}
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

const std::string BG_ENCRYPTION_AT_REST_MARKER_STRING = "Expecto..Patronum...BlobGranule";
/*
 * This is a stand-alone workload designed to validate blob granule correctness.
 * By enabling distinct ranges and writing to those parts of the key space, we can control what parts of the key space
 * are written to blob, and can validate that the granule data is correct at any desired version.
 */
struct BlobGranuleCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "BlobGranuleCorrectnessWorkload";
	bool doSetup;
	double testDuration;

	// parameters global across all clients
	int64_t targetByteRate;
	bool doMergeCheckAtEnd;
	bool doForceFlushing;

	std::vector<Reference<ThreadData>> directories;
	std::vector<Future<Void>> clients;
	DatabaseConfiguration config;

	BlobGranuleCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		doSetup = !clientId; // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 120.0);

		// randomize global test settings based on shared parameter to get similar workload across tests, but then vary
		// different parameters within those constraints
		int64_t randomness = sharedRandomNumber;

		doMergeCheckAtEnd = randomness % 10 == 0;
		randomness /= 10;

		if (g_network->isSimulated() && g_simulator->willRestart) {
			doMergeCheckAtEnd = false;
		}

		// randomize between low and high directory count
		int64_t targetDirectories = 1 + (randomness % 8);
		randomness /= 8;

		doForceFlushing = (randomness % 4);
		randomness /= 4;

		int64_t targetMyDirectories =
		    (targetDirectories / clientCount) + ((targetDirectories % clientCount > clientId) ? 1 : 0);

		if (g_network->isSimulated() && g_simulator->restarted) {
			// load directories later
			targetMyDirectories = 0;
		}

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

	ACTOR Future<TenantMapEntry> setUpTenant(Database cx, TenantName tenantName) {
		if (BGW_DEBUG) {
			fmt::print("Setting up blob granule range for tenant {0}\n", tenantName.printable());
		}

		Optional<TenantMapEntry> entry = wait(TenantAPI::createTenant(cx.getReference(), tenantName));
		ASSERT(entry.present());

		if (BGW_DEBUG) {
			fmt::print("Set up blob granule range for tenant {0}: {1}\n",
			           tenantName.printable(),
			           entry.get().prefix.printable());
		}

		return entry.get();
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	std::pair<uint32_t, uint32_t> parseKey(const KeyRef& key) {
		Tuple t = Tuple::unpackUserType(key, true);
		uint32_t k = t.getInt(0);
		Tuple::UserTypeStr userType = t.getUserType(1);
		std::stringstream ss(userType.str.toString());
		// ss.seekg(32); // skip first 32 zeroes?
		uint32_t id;
		ss >> id;

		return { k, id };
	}

	uint32_t parseVal(const ValueRef& val) {
		uint32_t v;
		sscanf(val.toString().substr(0, 8).c_str(), "%08x", &v);
		return v;
	}

	// because we don't have write versions for all of previous data, don't time travel back past restart and set all
	// write/clear versions to the read version of the data
	ACTOR Future<Void> loadPreviousDirectoryData(BlobGranuleCorrectnessWorkload* self,
	                                             Database cx,
	                                             Reference<ThreadData> threadData) {
		state uint32_t key;
		state uint32_t id;
		state uint32_t val;

		// read the old tenant's data and parse the keys from it
		state Transaction tr(cx, threadData->tenant);
		state KeyRange keyRange = normalKeys;
		state bool gotEOS = false;
		state int64_t totalRows = 0;
		state uint32_t lastKey = -1;
		state std::map<uint32_t, KeyData>::iterator lastKeyData = threadData->keyData.end();

		fmt::print("Loading previous directory data for {0}\n", threadData->directoryID);

		loop {
			try {
				state Version ver = wait(tr.getReadVersion());
				fmt::print("Dir {0}: RV={1}\n", threadData->directoryID, ver);

				state PromiseStream<Standalone<RangeResultRef>> results;
				state Future<Void> stream = tr.getRangeStream(results, keyRange, GetRangeLimits());
				loop {
					Standalone<RangeResultRef> res = waitNext(results.getFuture());
					totalRows += res.size();
					for (auto& it : res) {
						std::tie(key, id) = self->parseKey(it.key);

						if (key != lastKey) {
							auto insert = threadData->keyData.insert({ key, KeyData() });
							ASSERT(insert.second);
							lastKeyData = insert.first;
							lastKeyData->second.nextClearIdx = id;
							threadData->usedKeys.push_back(key);

							// all previous ids must have been cleared, fake clear version and value
							for (int clearedId = 0; clearedId < id; clearedId++) {
								lastKeyData->second.writes.emplace_back(ver, ver, 0, 20);
							}
							lastKey = key;
						}

						val = self->parseVal(it.value);

						// TODO REMOVE
						fmt::print("Dir {0}: ({1}, {2}) = {3}\n", threadData->directoryID, key, id, val);

						// insert new WriteData for key
						lastKeyData->second.writes.emplace_back(ver, MAX_VERSION, val, it.value.size());
					}

					if (!res.empty()) {
						keyRange = KeyRangeRef(keyAfter(res.back().key), keyRange.end);
					} else {
						// TODOREMOVE
						fmt::print(
						    "Empty range for [{0} - {1})\n", keyRange.begin.printable(), keyRange.end.printable());
					}
				}
			} catch (Error& e) {
				fmt::print("Error reading range for [{0} - {1}): {2}\n",
				           keyRange.begin.printable(),
				           keyRange.end.printable(),
				           e.name());
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}
				if (e.code() == error_code_end_of_stream) {
					gotEOS = true;
				} else {
					wait(tr.onError(e));
				}
			}

			if (gotEOS) {
				break;
			}
		}

		threadData->nextSeqKey = key + 1;

		fmt::print("Found {0} rows for  previous directory {1}\n", totalRows, threadData->directoryID);

		return Void();
	}

	ACTOR Future<Void> loadPreviousTenants(BlobGranuleCorrectnessWorkload* self, Database cx, BGTenantMap* tenantData) {
		state std::vector<std::pair<int64_t, TenantMapEntry>> allTenants;
		state Transaction tr(cx);
		state int i;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

				KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenantList =
				    wait(TenantMetadata::tenantMap().getRange(&tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
				ASSERT(tenantList.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !tenantList.more);

				ASSERT(!tenantList.results.empty());

				allTenants = tenantList.results;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		// TODO REMOVE
		fmt::print("Loaded {0} previous tenants\n", allTenants.size());

		// ignore tenants that aren't blobbified, might be default tenants created for other workloads
		state std::vector<std::pair<int64_t, TenantMapEntry>> blobbifiedTenants;
		for (i = 0; i < allTenants.size(); i++) {
			Reference<Tenant> tenant = makeReference<Tenant>(allTenants[i].first);
			Standalone<VectorRef<KeyRangeRef>> blobbifiedRanges = wait(cx->listBlobbifiedRanges(normalKeys, 1, tenant));
			if (!blobbifiedRanges.empty()) {
				blobbifiedTenants.push_back(allTenants[i]);
			}
		}
		tenantData->addTenants(blobbifiedTenants);
		ASSERT(!blobbifiedTenants.empty());

		// TODO REMOVE
		fmt::print("Found {0} previous blobbified tenants\n", blobbifiedTenants.size());

		// load tenants that exist, this actor takes the ones that have idx % total clients == my client id
		// init each tenant directory with the same randomized params that we do in the constructor
		int myDirectories = 0;
		// could do this with math but it's more obvious to read this way IMO
		for (i = self->clientId; i < blobbifiedTenants.size(); i += self->clientCount) {
			myDirectories++;
		}

		fmt::print("Client {0} loading {1} directories\n", self->clientId, myDirectories);

		if (myDirectories == 0) {
			return Void();
		}

		int denom = std::min(self->clientCount, (int)myDirectories);
		state int targetByteRate =
		    2 * SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES / deterministicRandom()->randomInt(1, 5) / denom;

		// either do equal across all of my directories, or skewed
		bool skewed = myDirectories > 1 && deterministicRandom()->random01() < 0.4;
		state int skewMultiplier;
		if (skewed) {
			// first directory has 1/2, second has 1/4, third has 1/8, etc...
			skewMultiplier = 2;
			targetByteRate /= 2;
		} else {
			skewMultiplier = 1;
			targetByteRate /= myDirectories;
		}

		state std::vector<Future<Void>> dataLoaders;
		for (i = self->clientId; i < blobbifiedTenants.size(); i += self->clientCount) {
			int dirId = atoi(blobbifiedTenants[i].second.tenantName.printable().c_str());
			if (BGW_DEBUG) {
				fmt::print("Client {0}/{1} re-creating directory {2}\n", self->clientId, self->clientCount, dirId);
			}
			state Reference<ThreadData> threadData = makeReference<ThreadData>(dirId, targetByteRate);

			wait(threadData->openTenant(cx));
			auto& tenantEntry = blobbifiedTenants[i].second;
			threadData->tenantEntry = tenantEntry;
			threadData->directoryRange = KeyRangeRef(tenantEntry.prefix, tenantEntry.prefix.withSuffix(normalKeys.end));

			self->directories.push_back(threadData);
			dataLoaders.push_back(self->loadPreviousDirectoryData(self, cx, threadData));

			targetByteRate /= skewMultiplier;
		}

		wait(waitForAll(dataLoaders));

		fmt::print("Client {0}/{1} recreated {2} directories\n", self->clientId, self->clientCount, dataLoaders.size());

		return Void();
	}

	ACTOR Future<Void> initializeTenants(BlobGranuleCorrectnessWorkload* self, Database cx, BGTenantMap* tenantData) {
		state int directoryIdx = 0;
		state std::vector<std::pair<int64_t, TenantMapEntry>> tenants;

		for (; directoryIdx < self->directories.size(); directoryIdx++) {
			// Set up the blob range first
			state TenantMapEntry tenantEntry = wait(self->setUpTenant(cx, self->directories[directoryIdx]->tenantName));
			wait(self->directories[directoryIdx]->openTenant(cx));
			self->directories[directoryIdx]->tenantEntry = tenantEntry;
			self->directories[directoryIdx]->directoryRange =
			    KeyRangeRef(tenantEntry.prefix, tenantEntry.prefix.withSuffix(normalKeys.end));
			tenants.push_back({ self->directories[directoryIdx]->tenant->id(), tenantEntry });
			bool _success = wait(cx->blobbifyRange(self->directories[directoryIdx]->directoryRange));
			ASSERT(_success);
		}
		tenantData->addTenants(tenants);

		return Void();
	}

	ACTOR Future<Void> _setup(Database cx, BlobGranuleCorrectnessWorkload* self) {
		if (self->doSetup) {
			// FIXME: run the actual FDBCLI command instead of copy/pasting its implementation
			wait(success(ManagementAPI::changeConfig(cx.getReference(), "blob_granules_enabled=1", true)));
		}

		DatabaseConfiguration dbConfig = wait(getDatabaseConfiguration(cx));
		if (g_network->isSimulated() && dbConfig.encryptionAtRestMode.isEncryptionEnabled()) {
			TraceEvent("EncryptionAtRestPlainTextMarkerCheckEnabled")
			    .detail("EncryptionMode", dbConfig.encryptionAtRestMode.toString())
			    .detail("DataAtRestMarker", BG_ENCRYPTION_AT_REST_MARKER_STRING);
			g_simulator->dataAtRestPlaintextMarker = BG_ENCRYPTION_AT_REST_MARKER_STRING;
		}

		state BGTenantMap tenantData(self->dbInfo);

		if (g_network->isSimulated() && g_simulator->restarted) {
			wait(self->loadPreviousTenants(self, cx, &tenantData));
		} else {
			wait(self->initializeTenants(self, cx, &tenantData));
		}

		// wait for tenant data to be loaded
		state Reference<GranuleTenantData> data;
		state int directoryIdx = 0;
		for (directoryIdx = 0; directoryIdx < self->directories.size(); directoryIdx++) {
			wait(store(data, tenantData.getDataForGranule(self->directories[directoryIdx]->directoryRange)));
			wait(data->bstoreLoaded.getFuture());
			wait(delay(0));
			self->directories[directoryIdx]->bstore = data->bstore;
		}

		return Void();
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
			state Transaction tr(cx, threadData->tenant);
			try {
				Version rv = wait(self->doGrv(&tr));
				state Version readVersion = rv;
				std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob = wait(readFromBlob(
				    cx, threadData->bstore, normalKeys /* tenant handles range */, 0, readVersion, threadData->tenant));
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

	void logKey(Optional<Key> key) {
		if (!key.present()) {
			fmt::print("<missing>\n");
		} else {
			uint32_t k, id;
			std::tie(k, id) = parseKey(key.get());
			fmt::print("({0}, {1}) : {2}\n", k, id, key.get().printable());
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
			fmt::print("    last correct: ");
			logKey(lastMatching);
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
			fmt::print("    Expected Key: ");
			logKey(expectedKey);
			fmt::print("      Actual Key: ");
			logKey(blobKey);
		}

		fmt::print("Chunks: {0}\n", blob.second.size());
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
			fmt::print("DBG READ: [{0} - {1}) @ {2} ({3} rows)\n",
			           format("%08x", startKeyInclusive),
			           format("%08x", endKeyExclusive),
			           readVersion,
			           blob.first.size());
		}

		// because each chunk could be separately collapsed or not if we set beginVersion, we have to track it by chunk
		KeyRangeMap<Version> beginVersionByChunk;
		beginVersionByChunk.insert(normalKeys, 0);
		int beginCollapsed = 0;
		int beginNotCollapsed = 0;
		Key lastBeginKey = ""_sr;
		for (auto& chunk : blob.second) {
			KeyRange beginVersionRange;
			if (chunk.tenantPrefix.present()) {
				beginVersionRange = KeyRangeRef(chunk.keyRange.begin.removePrefix(chunk.tenantPrefix.get()),
				                                chunk.keyRange.end.removePrefix(chunk.tenantPrefix.get()));
			} else {
				beginVersionRange = chunk.keyRange;
			}

			if (!chunk.snapshotFile.present()) {
				ASSERT(beginVersion > 0);
				ASSERT(chunk.snapshotVersion == invalidVersion);
				beginCollapsed++;

				beginVersionByChunk.insert(beginVersionRange, beginVersion);
			} else {
				ASSERT(chunk.snapshotVersion != invalidVersion);
				if (beginVersion > 0) {
					beginNotCollapsed++;
				}
			}

			// Validate boundary alignment.
			threadData->validateGranuleBoundary(beginVersionRange.begin, beginVersionRange.end, lastBeginKey);
			lastBeginKey = beginVersionRange.begin;
		}
		CODE_PROBE(
		    beginCollapsed > 0, "BGCorrectness got collapsed request with beginVersion > 0", probe::decoration::rare);
		CODE_PROBE(beginNotCollapsed > 0,
		           "BGCorrectness got un-collapsed request with beginVersion > 0",
		           probe::decoration::rare);
		CODE_PROBE(beginCollapsed > 0 && beginNotCollapsed > 0,
		           "BGCorrectness got both collapsed and uncollapsed in the same request!",
		           probe::decoration::rare);

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
		state KeyRange range;

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

				// sometimes force single key read, for edge case
				state uint32_t endKey;
				if (deterministicRandom()->random01() < 0.01) {
					endKey = startKey + 1;
				} else {
					int targetQueryBytes = (deterministicRandom()->randomInt(1, 20) * targetBytesReadPerQuery) / 10;
					int estimatedQueryBytes = 0;
					for (; estimatedQueryBytes < targetQueryBytes && endKeyIt != threadData->keyData.end();
					     endKeyIt++) {
						// iterate forward until end or target keys have passed
						estimatedQueryBytes += (1 + endKeyIt->second.writes.size() - endKeyIt->second.nextClearIdx) *
						                       threadData->targetValLength;
					}

					if (endKeyIt == threadData->keyData.end()) {
						endKey = std::numeric_limits<uint32_t>::max();
					} else {
						endKey = endKeyIt->first;
					}
				}

				range = KeyRangeRef(threadData->getKey(startKey, 0), threadData->getKey(endKey, 0));

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

				threadData->maybeReopenTenant(cx);

				std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob =
				    wait(readFromBlob(cx, threadData->bstore, range, beginVersion, readVersion, threadData->tenant));
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
					if (BGW_DEBUG) {
						fmt::print("ERROR: TTO for [{0} - {1}) @ {2} for tenant {3}\n",
						           range.begin.printable(),
						           range.end.printable(),
						           readVersion,
						           printable(threadData->tenant->description()));
					}
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
			state Transaction tr(cx, threadData->tenant);

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
						key = threadData->nextSeqKey;
						++threadData->nextSeqKey;
					} else {
						key = std::numeric_limits<uint32_t>::max();
						while (key == std::numeric_limits<uint32_t>::max() ||
						       threadData->keyData.find(key) != threadData->keyData.end()) {
							// leave half of the end of the keyspace in case restarting test switches to sequential
							key = deterministicRandom()->randomUInt32() / 2;
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
						if (DEBUG_KEY_OP(threadData->directoryID, std::get<0>(it))) {
							fmt::print("DBG: {0} PREWRITE ({1}, {2}) = {3}:{4}\n",
							           threadData->directoryID,
							           std::get<0>(it),
							           std::get<1>(it),
							           std::get<2>(it),
							           std::get<3>(it));
						}
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
					fmt::print("Writer error {0}\n", e.name());
					wait(tr.onError(e));
				}
			}

			Version commitVersion = tr.getCommittedVersion();
			// TODO REMOVE
			fmt::print("Writer committed @ {0}\n", commitVersion);

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
			it->summaryClient = validateGranuleSummaries(cx, normalKeys, it->tenant, it->triggerSummaryComplete);
			if (doForceFlushing && deterministicRandom()->random01() < 0.25) {
				it->forceFlushingClient =
				    validateForceFlushing(cx, it->directoryRange, testDuration, it->triggerSummaryComplete);
			} else {
				it->forceFlushingClient = Future<Void>(Void());
			}
			clients.push_back(timeout(writeWorker(this, start, cx, it), testDuration, Void()));
			clients.push_back(timeout(readWorker(this, start, cx, it), testDuration, Void()));
		}
		return delay(testDuration);
	}

	ACTOR Future<Void> checkTenantRanges(BlobGranuleCorrectnessWorkload* self,
	                                     Database cx,
	                                     Reference<ThreadData> threadData) {
		// check that reading ranges with tenant name gives valid result of ranges just for tenant, with no tenant
		// prefix
		loop {
			state Transaction tr(cx, threadData->tenant);
			try {
				Standalone<VectorRef<KeyRangeRef>> ranges = wait(tr.getBlobGranuleRanges(normalKeys, 1000000));
				ASSERT(ranges.size() >= 1 && ranges.size() < 1000000);
				ASSERT(ranges.front().begin == normalKeys.begin);
				ASSERT(ranges.back().end == normalKeys.end);
				for (int i = 0; i < ranges.size() - 1; i++) {
					ASSERT(ranges[i].end == ranges[i + 1].begin);
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<bool> checkDirectory(Database cx,
	                                  BlobGranuleCorrectnessWorkload* self,
	                                  Reference<ThreadData> threadData) {

		if (threadData->triggerSummaryComplete.canBeSet()) {
			threadData->triggerSummaryComplete.send(Void());
		}
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
			std::pair<RangeResult, Standalone<VectorRef<BlobGranuleChunkRef>>> blob = wait(readFromBlob(
			    cx, threadData->bstore, normalKeys /*tenant handles range*/, 0, readVersion, threadData->tenant));
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
		// read granule ranges with tenant and validate
		if (BGW_DEBUG) {
			fmt::print("Directory {0} checking tenant ranges\n", threadData->directoryID);
		}
		wait(self->checkTenantRanges(self, cx, threadData));

		state bool initialCheck = result;
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

		if (self->clientId == 0 && SERVER_KNOBS->BG_ENABLE_MERGING && self->doMergeCheckAtEnd) {
			CODE_PROBE(true, "BGCorrectness clearing database and awaiting merge", probe::decoration::rare);
			wait(clearAndAwaitMerge(cx, threadData->directoryRange));
		}

		// validate that summary completes without error
		wait(threadData->summaryClient && threadData->forceFlushingClient);

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

		// do feed cleanup check only after data is guaranteed to be available for each granule
		state Future<Void> checkFeedCleanupFuture;
		if (self->clientId == 0) {
			checkFeedCleanupFuture = checkFeedCleanup(cx, BGW_DEBUG);
		} else {
			checkFeedCleanupFuture = Future<Void>(Void());
		}
		wait(checkFeedCleanupFuture);
		return allSuccessful;
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<BlobGranuleCorrectnessWorkload> BlobGranuleCorrectnessWorkloadFactory;
