/*
 * ApiCorrectness.actor.cpp
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

#include "fdbserver/QuietDatabase.h"

#include "fdbserver/MutationTracking.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/ApiWorkload.h"
#include "fdbserver/workloads/MemoryKeyValueStore.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// An enum of API operation types used in the random test
enum OperationType { SET, GET, GET_RANGE, GET_RANGE_SELECTOR, GET_KEY, CLEAR, CLEAR_RANGE, UNINITIALIZED };

// A workload that executes the NativeAPIs functions and verifies that their outcomes are correct
struct ApiCorrectnessWorkload : ApiWorkload {

private:
// Enable to track the activity on a particular key
#if CENABLED(0, NOT_IN_CLEAN)
#define targetKey LiteralStringRef( ??? )

	void debugKey(KeyRef key, std::string context) {
		if (key == targetKey)
			TraceEvent("ApiCorrectnessDebugKey").detail("Context", context).detail("Key", printable(key));
	}

	void debugKey(KeyRangeRef keyRange, std::string context) {
		if (keyRange.contains(targetKey))
			TraceEvent("ApiCorrectnessDebugKey")
			    .detail("Context", context)
			    .detail("Key", printable(targetKey))
			    .detail("RangeBegin", printable(keyRange.begin))
			    .detail("RangeEnd", printable(keyRange.end));
	}

#else
	void debugKey(KeyRef key, std::string context) {}
	void debugKey(KeyRangeRef keyRange, std::string context) {}

#endif

public:
	// The number of gets that should be performed
	int numGets;

	// The number of getRanges that should be performed
	int numGetRanges;

	// The number of getRanges using key selectors that should be performed
	int numGetRangeSelectors;

	// The number of getKeys that should be performed
	int numGetKeys;

	// The number of clears that should be performed
	int numClears;

	// The number of clears using key ranges that should be performed
	int numClearRanges;

	// The smallest legal size of the database after a clear.  A smaller size will trigger a database reset
	int minSizeAfterClear;

	// The maximum number of keys that can be in this client's key space when performing the random test
	int maxRandomTestKeys;

	// The amount of time to run the random tests
	double randomTestDuration;

	// The maximum number of keys operated on in a transaction; used to prevent transaction_too_old errors
	int maxKeysPerTransaction;

	// The number of API calls made by the random test
	PerfIntCounter numRandomOperations;

	// The API being used by this client
	TransactionType transactionType;

	// Maximum time to reset DB to the original state
	double resetDBTimeout;

	ApiCorrectnessWorkload(WorkloadContext const& wcx)
	  : ApiWorkload(wcx), numRandomOperations("Num Random Operations") {
		numGets = getOption(options, LiteralStringRef("numGets"), 1000);
		numGetRanges = getOption(options, LiteralStringRef("numGetRanges"), 100);
		numGetRangeSelectors = getOption(options, LiteralStringRef("numGetRangeSelectors"), 100);
		numGetKeys = getOption(options, LiteralStringRef("numGetKeys"), 100);
		numClears = getOption(options, LiteralStringRef("numClears"), 100);
		numClearRanges = getOption(options, LiteralStringRef("numClearRanges"), 100);
		minSizeAfterClear = getOption(options, LiteralStringRef("minSizeAfterClear"), (int)(0.1 * numKeys));

		maxRandomTestKeys = getOption(options, LiteralStringRef("maxRandomTestKeys"), numKeys);
		randomTestDuration = getOption(options, LiteralStringRef("randomTestDuration"), 60.0);

		int maxTransactionBytes = getOption(options, LiteralStringRef("maxTransactionBytes"), 500000);
		maxKeysPerTransaction = std::max(1, maxTransactionBytes / (maxValueLength + maxLongKeyLength));

		resetDBTimeout = getOption(options, LiteralStringRef("resetDBTimeout"), 1800.0);

		if (maxTransactionBytes > 500000) {
			TraceEvent("RemapEventSeverity")
			    .detail("TargetEvent", "LargePacketSent")
			    .detail("OriginalSeverity", SevWarnAlways)
			    .detail("NewSeverity", SevInfo);
			TraceEvent("RemapEventSeverity")
			    .detail("TargetEvent", "LargePacketReceived")
			    .detail("OriginalSeverity", SevWarnAlways)
			    .detail("NewSeverity", SevInfo);
			TraceEvent("RemapEventSeverity")
			    .detail("TargetEvent", "LargeTransaction")
			    .detail("OriginalSeverity", SevWarnAlways)
			    .detail("NewSeverity", SevInfo);
			TraceEvent("RemapEventSeverity")
			    .detail("TargetEvent", "DiskQueueMemoryWarning")
			    .detail("OriginalSeverity", SevWarnAlways)
			    .detail("NewSeverity", SevInfo);
		}
	}

	~ApiCorrectnessWorkload() override {}

	std::string description() const override { return "ApiCorrectness"; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Number of Random Operations Performed", numRandomOperations.getValue(), Averaged::False);
	}

	ACTOR Future<Void> performSetup(Database cx, ApiCorrectnessWorkload* self) {
		// Choose a random transaction type (NativeAPI, ReadYourWrites, ThreadSafe, MultiVersion)
		std::vector<TransactionType> types;
		types.push_back(NATIVE);
		types.push_back(READ_YOUR_WRITES);
		types.push_back(THREAD_SAFE);
		types.push_back(MULTI_VERSION);

		wait(self->chooseTransactionFactory(cx, types));

		return Void();
	}

	Future<Void> performSetup(Database const& cx) override { return performSetup(cx, this); }

	ACTOR Future<Void> performTest(Database cx, Standalone<VectorRef<KeyValueRef>> data, ApiCorrectnessWorkload* self) {
		// Run the scripted test for a maximum of 10 minutes
		wait(timeout(self->runScriptedTest(self, data), 600, Void()));

		if (!self->hasFailed()) {
			// Return database to original state (for a maximum of resetDBTimeout seconds)
			try {
				wait(timeoutError(::success(self->runSet(data, self)), self->resetDBTimeout));
			} catch (Error& e) {
				if (e.code() == error_code_timed_out) {
					if (!self->hasFailed())
						self->testFailure("Timeout during database reset");

					return Void();
				}

				throw;
			}

			// Run the random test for the user-specified duration
			wait(timeout(self->runRandomTest(self, data), self->randomTestDuration, Void()));
		}

		return Void();
	}

	Future<Void> performTest(Database const& cx, Standalone<VectorRef<KeyValueRef>> const& data) override {
		return performTest(cx, data, this);
	}

	// Run a scripted set of API operations
	ACTOR Future<Void> runScriptedTest(ApiCorrectnessWorkload* self, VectorRef<KeyValueRef> data) {
		// Test the set function
		bool setResult = wait(self->runSet(data, self));
		if (!setResult)
			return Void();

		// Test the get function
		wait(::success(self->runGet(data, self->numGets, self)));

		// Test the getRange function
		state int i;
		for (i = 0; i < self->numGetRanges; i++)
			wait(::success(self->runGetRange(data, self)));

		// Test the getRange function using key selectors
		for (i = 0; i < self->numGetRangeSelectors; i++)
			wait(::success(self->runGetRangeSelector(data, self)));

		// Test the getKey function
		wait(::success(self->runGetKey(data, self->numGetKeys, self)));

		// Test the clear function
		bool clearResult = wait(self->runClear(data, self->numClears, self));
		if (!clearResult)
			return Void();

		// Test the clear function using keyRanges
		for (i = 0; i < self->numClearRanges; i++) {
			// Alternate restoring the database to its original state and clearing a single range
			if (self->store.size() < self->minSizeAfterClear) {
				bool resetResult = wait(self->runSet(data, self));
				if (!resetResult)
					return Void();
			}

			bool clearRangeResults = wait(self->runClearRange(data, self));
			if (!clearRangeResults)
				return Void();
		}

		return Void();
	}

	// Generate and execute a sequence of random operations
	ACTOR Future<Void> runRandomTest(ApiCorrectnessWorkload* self, Standalone<VectorRef<KeyValueRef>> data) {
		loop {
			double setProbability = 1 - ((double)self->store.size()) / self->maxRandomTestKeys;
			int pdfArray[] = { 0,
				               (int)(100 * setProbability),
				               100,
				               50,
				               50,
				               20,
				               (int)(100 * (1 - setProbability)),
				               (int)(10 * (1 - setProbability)) };
			std::vector<int> pdf = std::vector<int>(pdfArray, pdfArray + 8);

			OperationType operation = UNINITIALIZED;

			// Choose a random operation type (SET, GET, GET_RANGE, GET_RANGE_SELECTOR, GET_KEY, CLEAR, CLEAR_RANGE).
			int totalDensity = 0;
			for (int i = 0; i < pdf.size(); i++)
				totalDensity += pdf[i];

			int cumulativeDensity = 0;
			int random = deterministicRandom()->randomInt(0, totalDensity);
			for (int i = 0; i < pdf.size() - 1; i++) {
				if (cumulativeDensity + pdf[i] <= random && random < cumulativeDensity + pdf[i] + pdf[i + 1]) {
					operation = (OperationType)i;
					break;
				}

				cumulativeDensity += pdf[i];
			}
			ASSERT(operation != UNINITIALIZED);

			++self->numRandomOperations;

			// Test the set operation
			if (operation == SET) {
				bool useShortKeys = deterministicRandom()->randomInt(0, 2) == 1;
				int minKeyLength = useShortKeys ? self->minShortKeyLength : self->minLongKeyLength;
				int maxKeyLength = useShortKeys ? self->maxShortKeyLength : self->maxLongKeyLength;

				state Standalone<VectorRef<KeyValueRef>> newData =
				    self->generateData(std::min((uint64_t)100, self->maxRandomTestKeys - self->store.size()),
				                       minKeyLength,
				                       maxKeyLength,
				                       self->minValueLength,
				                       self->maxValueLength,
				                       self->clientPrefix,
				                       true);

				data.append_deep(data.arena(), newData.begin(), newData.size());

				bool result = wait(self->runSet(newData, self));
				if (!result)
					return Void();
			}

			// Test the get operation
			else if (operation == GET) {
				bool result = wait(self->runGet(data, 10, self));
				if (!result)
					return Void();
			}

			// Test the getRange operation
			else if (operation == GET_RANGE) {
				bool result = wait(self->runGetRange(data, self));
				if (!result)
					return Void();
			}

			// Test the getRange operation with key selectors
			else if (operation == GET_RANGE_SELECTOR) {
				bool result = wait(self->runGetRangeSelector(data, self));
				if (!result)
					return Void();
			}

			// Test the getKey operation
			else if (operation == GET_KEY) {
				bool result = wait(self->runGetKey(data, 10, self));
				if (!result)
					return Void();
			}

			// Test the clear operation
			else if (operation == CLEAR) {
				bool result = wait(self->runClear(data, 10, self));
				if (!result)
					return Void();
			}

			// Test the clear operation (using key range)
			else if (operation == CLEAR_RANGE) {
				bool result = wait(self->runClearRange(data, self));
				if (!result)
					return Void();
			}
		}
	}

	// Adds the key-value pairs in data to the database and memory store
	ACTOR Future<bool> runSet(VectorRef<KeyValueRef> data, ApiCorrectnessWorkload* self) {
		state int currentIndex = 0;
		while (currentIndex < data.size()) {
			state Reference<TransactionWrapper> transaction = self->createTransaction();

			// Set keys in the database
			loop {
				try {
					// For now, make this transaction self-conflicting to avoid commit errors
					Optional<Value> value = wait(transaction->get(data[currentIndex].key));

					for (int i = currentIndex; i < std::min(currentIndex + self->maxKeysPerTransaction, data.size());
					     i++) {
						transaction->addReadConflictRange(singleKeyRange(data[i].key));
						transaction->set(data[i].key, data[i].value);
					}

					wait(transaction->commit());
					for (int i = currentIndex; i < std::min(currentIndex + self->maxKeysPerTransaction, data.size());
					     i++)
						DEBUG_MUTATION("ApiCorrectnessSet",
						               transaction->getCommittedVersion(),
						               MutationRef(MutationRef::DebugKey, data[i].key, data[i].value));

					currentIndex += self->maxKeysPerTransaction;
					break;
				} catch (Error& e) {
					wait(transaction->onError(e));
				}
			}
		}

		// Set keys in memory
		for (int i = 0; i < data.size(); i++) {
			self->store.set(data[i].key, data[i].value);
			self->debugKey(data[i].key, "Set");
		}

		// Check that the database and memory store are the same
		bool result = wait(self->compareDatabaseToMemory());
		if (!result)
			self->testFailure("Set resulted in incorrect database");

		return result;
	}

	// Gets a specified number of values from the database and memory store and compares them, returning true if all
	// results were the same
	ACTOR Future<bool> runGet(VectorRef<KeyValueRef> data, int numReads, ApiCorrectnessWorkload* self) {
		// Generate a set of random keys to get
		state Standalone<VectorRef<KeyRef>> keys;
		for (int i = 0; i < numReads; i++)
			keys.push_back_deep(keys.arena(), self->selectRandomKey(data, 0.9));

		state std::vector<Optional<Value>> values;

		state int currentIndex = 0;
		while (currentIndex < keys.size()) {
			state Reference<TransactionWrapper> transaction = self->createTransaction();

			// Get the values from the database
			loop {
				try {
					state std::vector<Future<Optional<Value>>> dbValueFutures;
					for (int i = currentIndex; i < std::min(currentIndex + self->maxKeysPerTransaction, keys.size());
					     i++)
						dbValueFutures.push_back(transaction->get(keys[i]));

					wait(waitForAll(dbValueFutures));

					for (int i = 0; i < dbValueFutures.size(); i++)
						values.push_back(dbValueFutures[i].get());

					currentIndex += self->maxKeysPerTransaction;

					break;
				} catch (Error& e) {
					wait(transaction->onError(e));
				}
			}
		}

		bool result = true;

		// Get the values from the memory store and compare them
		for (int i = 0; i < keys.size(); i++) {
			if (values[i] != self->store.get(keys[i])) {
				result = false;
				break;
			}
		}

		if (!result)
			self->testFailure("Get returned incorrect results");

		return result;
	}

	// Gets a single range of values from the database and memory stores and compares them, returning true if the
	// results were the same
	ACTOR Future<bool> runGetRange(VectorRef<KeyValueRef> data, ApiCorrectnessWorkload* self) {
		state Reverse reverse = deterministicRandom()->coinflip();

		// Generate a random range
		Key key = self->selectRandomKey(data, 0.5);
		Key key2 = self->selectRandomKey(data, 0.5);

		state Key start = std::min(key, key2);
		state Key end = std::max(key, key2);

		// Generate a random maximum number of results
		state int limit = deterministicRandom()->randomInt(0, 101);

		// Get the range from memory
		state RangeResult storeResults = self->store.getRange(KeyRangeRef(start, end), limit, reverse);

		// Get the range from the database
		state RangeResult dbResults;
		state Version readVersion;

		state Reference<TransactionWrapper> transaction = self->createTransaction();

		loop {
			try {
				Version version = wait(transaction->getReadVersion());
				readVersion = version;

				KeyRangeRef range(start, end);
				RangeResult rangeResults = wait(transaction->getRange(range, limit, reverse));
				dbResults = rangeResults;
				break;
			} catch (Error& e) {
				wait(transaction->onError(e));
			}
		}

		// Compare the ranges
		bool result = self->compareResults(dbResults, storeResults, readVersion);
		if (!result)
			self->testFailure("GetRange returned incorrect results");

		return result;
	}

	// Gets a single range of values using key selectors from the database and memory store and compares them, returning
	// true if the results were the same
	ACTOR Future<bool> runGetRangeSelector(VectorRef<KeyValueRef> data, ApiCorrectnessWorkload* self) {
		state Reverse reverse = deterministicRandom()->coinflip();

		KeySelector selectors[2];
		Key keys[2];

		int maxSelectorAttempts = 100;
		int currentSelectorAttempts = 0;

		// Generate a random pair of key selectors and determine the keys they point to
		// Don't use key selectors which would return results outside the key-space of this client unless this client
		// is the first or last client
		for (int i = 0; i < 2; i++) {
			loop {
				// Gradually decrease the maximum offset to increase the likelihood of finding a valid key selector in a
				// small store
				selectors[i] =
				    self->generateKeySelector(data, std::min(100, maxSelectorAttempts - currentSelectorAttempts));
				keys[i] = self->store.getKey(selectors[i]);

				if (keys[i].startsWith(StringRef(self->clientPrefix)) ||
				    (keys[i].size() == 0 && self->clientPrefixInt == 0) ||
				    (keys[i].startsWith(LiteralStringRef("\xff")) && self->clientPrefixInt == self->clientCount - 1)) {
					break;
				}

				// Don't loop forever trying to generate valid key selectors if there are no keys in the store
				if (++currentSelectorAttempts == maxSelectorAttempts)
					return true;
			}
		}

		state KeySelector startSelector;
		state KeySelector endSelector;

		state Key startKey;
		state Key endKey;

		// Make sure startKey is less than endKey
		if (keys[0] < keys[1]) {
			startSelector = selectors[0];
			startKey = keys[0];
			endSelector = selectors[1];
			endKey = keys[1];
		} else {
			startSelector = selectors[1];
			startKey = keys[1];
			endSelector = selectors[0];
			endKey = keys[0];
		}

		// Choose a random maximum number of results
		state int limit = deterministicRandom()->randomInt(0, 101);

		// Get the range from the memory store
		state RangeResult storeResults = self->store.getRange(KeyRangeRef(startKey, endKey), limit, reverse);

		// Get the range from the database
		state RangeResult dbResults;

		state Reference<TransactionWrapper> transaction = self->createTransaction();
		state Version readVersion;

		loop {
			try {
				Version version = wait(transaction->getReadVersion());
				readVersion = version;

				RangeResult range = wait(transaction->getRange(startSelector, endSelector, limit, reverse));

				if (endKey == self->store.endKey()) {
					for (int i = 0; i < range.size(); i++) {
						// Don't include results in the 0xFF key-space
						if (!range[i].key.startsWith(LiteralStringRef("\xff")))
							dbResults.push_back_deep(dbResults.arena(), range[i]);
					}
					if (reverse && dbResults.size() < storeResults.size()) {
						storeResults.resize(storeResults.arena(), dbResults.size());
					}
				} else
					dbResults = range;

				break;
			} catch (Error& e) {
				wait(transaction->onError(e));
			}
		}

		// Compare the results
		bool result = self->compareResults(dbResults, storeResults, readVersion);

		if (!result)
			self->testFailure("GetRange (KeySelector) returned incorrect results");

		return result;
	}

	// Gets a specified number of keys from the database and memory store and compares them, returning true if all
	// results were the same
	ACTOR Future<bool> runGetKey(VectorRef<KeyValueRef> data, int numGetKeys, ApiCorrectnessWorkload* self) {
		// Generate a set of random key selectors
		state Standalone<VectorRef<KeySelectorRef>> selectors;
		for (int i = 0; i < numGetKeys; i++)
			selectors.push_back_deep(selectors.arena(), self->generateKeySelector(data, 100));

		state Standalone<VectorRef<KeyRef>> keys;

		state int currentIndex = 0;
		while (currentIndex < selectors.size()) {
			// Get the keys from the database
			state Reference<TransactionWrapper> transaction = self->createTransaction();

			loop {
				try {
					state std::vector<Future<Standalone<KeyRef>>> dbKeyFutures;
					for (int i = currentIndex;
					     i < std::min(currentIndex + self->maxKeysPerTransaction, selectors.size());
					     i++)
						dbKeyFutures.push_back(transaction->getKey(selectors[i]));

					wait(waitForAll(dbKeyFutures));

					for (int i = 0; i < dbKeyFutures.size(); i++)
						keys.push_back_deep(keys.arena(), dbKeyFutures[i].get());

					currentIndex += self->maxKeysPerTransaction;

					break;
				} catch (Error& e) {
					wait(transaction->onError(e));
				}
			}
		}

		state bool result = true;

		// Get the keys from the memory store and compare them
		state int i;
		for (i = 0; i < selectors.size(); i++) {
			Key key = self->store.getKey(selectors[i]);
			if (keys[i].startsWith(StringRef(self->clientPrefix)) && keys[i] != key)
				result = false;
			else if (keys[i] < StringRef(self->clientPrefix) && key != self->store.startKey())
				result = false;
			else if (keys[i] > StringRef(self->clientPrefix + "\xff") && key != self->store.endKey())
				result = false;

			// If there was a failure, print some debugging info about the failed key
			if (!result) {
				printf("Bad result for key selector %s: db=%s, mem=%s\n",
				       selectors[i].toString().c_str(),
				       printable(keys[i]).c_str(),
				       printable(key).c_str());
				state int dir = selectors[i].offset > 0 ? 1 : -1;
				state int j;
				for (j = 0; j <= abs(selectors[i].offset); j++) {
					state KeySelector sel = KeySelectorRef(selectors[i].getKey(), selectors[i].orEqual, j * dir);
					state Key storeKey = self->store.getKey(sel);

					state Reference<TransactionWrapper> tr = self->createTransaction();

					state Key dbKey;
					loop {
						try {
							Key key = wait(tr->getKey(sel));
							dbKey = key;
							break;
						} catch (Error& e) {
							wait(tr->onError(e));
						}
					}

					if (!(storeKey == self->store.startKey() && dbKey < StringRef(self->clientPrefix)) &&
					    !(storeKey == self->store.endKey() && dbKey > StringRef(self->clientPrefix + "\xff")))
						printf("Offset %d: db=%s, mem=%s\n",
						       j * dir,
						       printable(dbKey).c_str(),
						       printable(storeKey).c_str());
				}

				break;
			}
		}

		if (!result)
			self->testFailure("GetKey returned incorrect results");

		return result;
	}

	// Clears a specified number of keys from the database and memory store
	ACTOR Future<bool> runClear(VectorRef<KeyValueRef> data, int numClears, ApiCorrectnessWorkload* self) {
		// Generate a random set of keys to clear
		state Standalone<VectorRef<KeyRef>> keys;
		for (int i = 0; i < numClears; i++)
			keys.push_back_deep(keys.arena(), self->selectRandomKey(data, 0.9));

		state int currentIndex = 0;
		while (currentIndex < keys.size()) {
			// Clear the keys from the database
			state Reference<TransactionWrapper> transaction = self->createTransaction();
			loop {
				try {
					// For now, make this transaction self-conflicting to avoid commit errors
					Optional<Value> value = wait(transaction->get(keys[0]));

					for (int i = currentIndex; i < std::min(currentIndex + self->maxKeysPerTransaction, keys.size());
					     i++) {
						transaction->addReadConflictRange(singleKeyRange(keys[i]));
						transaction->clear(keys[i]);
					}

					wait(transaction->commit());
					for (int i = currentIndex; i < std::min(currentIndex + self->maxKeysPerTransaction, keys.size());
					     i++)
						DEBUG_MUTATION("ApiCorrectnessClear",
						               transaction->getCommittedVersion(),
						               MutationRef(MutationRef::DebugKey, keys[i], StringRef()));

					currentIndex += self->maxKeysPerTransaction;
					break;
				} catch (Error& e) {
					wait(transaction->onError(e));
				}
			}
		}

		// Clear the keys from the memory store
		for (int i = 0; i < keys.size(); i++) {
			self->store.clear(keys[i]);
			self->debugKey(keys[i], "Clear");
		}

		// Check that the database and memory store are the same
		bool result = wait(self->compareDatabaseToMemory());
		if (!result)
			self->testFailure("Clear resulted in incorrect database");

		return result;
	}

	// Clears a single range of keys from the database and memory store
	ACTOR Future<bool> runClearRange(VectorRef<KeyValueRef> data, ApiCorrectnessWorkload* self) {
		// Generate a random range to clear
		Key key = self->selectRandomKey(data, 0.5);
		Key key2 = self->selectRandomKey(data, 0.5);

		state Key start = std::min(key, key2);
		state Key end = std::max(key, key2);

		// Clear the range in memory
		self->store.clear(KeyRangeRef(start, end));

		// Clear the range in the database
		state Reference<TransactionWrapper> transaction = self->createTransaction();

		loop {
			try {
				// For now, make this transaction self-conflicting to avoid commit errors
				Optional<Value> value = wait(transaction->get(start));

				state KeyRangeRef range(start, end);
				if (!range.empty()) {
					transaction->addReadConflictRange(range);
				}
				transaction->clear(range);
				wait(transaction->commit());
				DEBUG_KEY_RANGE("ApiCorrectnessClear", transaction->getCommittedVersion(), range);
				break;
			} catch (Error& e) {
				wait(transaction->onError(e));
			}
		}

		self->debugKey(KeyRangeRef(start, end), "ClearRange");

		// Check that the database and memory store are the same
		bool result = wait(self->compareDatabaseToMemory());
		if (!result)
			self->testFailure("Clear (range) resulted in incorrect database");

		return result;
	}
};

WorkloadFactory<ApiCorrectnessWorkload> ApiCorrectnessWorkloadFactory("ApiCorrectness");
