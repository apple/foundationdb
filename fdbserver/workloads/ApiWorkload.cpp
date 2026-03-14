/*
 * ApiWorkload.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/workloads/ApiWorkload.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/MultiVersionTransaction.h"

#include "fdbrpc/simulator.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"

#include <cinttypes>
#include "fmt/format.h"

// Clears the keyspace used by this test
Future<Void> clearKeyspace(ApiWorkload* self) {
	while (true) {
		Reference<TransactionWrapper> transaction = self->createTransaction();
		Error err;
		try {
			KeyRange range(KeyRangeRef(StringRef(fmt::format("{:010}", self->clientPrefixInt)),
			                           StringRef(fmt::format("{:010}", self->clientPrefixInt + 1))));

			transaction->clear(range);
			co_await transaction->commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await transaction->onError(err);
	}
}

Future<Void> ApiWorkload::clearKeyspace() {
	return ::clearKeyspace(this);
}

Future<Void> setup(Database cx, ApiWorkload* self) {
	self->transactionFactory = Reference<TransactionFactoryInterface>(
	    new TransactionFactory<FlowTransactionWrapper<Transaction>, const Database>(cx, cx, false));

	// Clear keyspace before running
	co_await timeoutError(self->clearKeyspace(), 600);
	co_await self->performSetup(cx);
}

Future<Void> ApiWorkload::setup(Database const& cx) {
	if (clientId < maxClients || maxClients < 0)
		return ::setup(cx, this);

	return Void();
}

Future<Void> start(Database cx, ApiWorkload* self) {
	// Generate the data to store in this client's key-space
	Standalone<VectorRef<KeyValueRef>> data = self->generateData(self->numKeys * self->shortKeysRatio,
	                                                             self->minShortKeyLength,
	                                                             self->maxShortKeyLength,
	                                                             self->minValueLength,
	                                                             self->maxValueLength,
	                                                             self->clientPrefix);
	Standalone<VectorRef<KeyValueRef>> bigKeyData = self->generateData(self->numKeys * (1 - self->shortKeysRatio),
	                                                                   self->minLongKeyLength,
	                                                                   self->maxLongKeyLength,
	                                                                   self->minValueLength,
	                                                                   self->maxValueLength,
	                                                                   self->clientPrefix);

	data.append_deep(data.arena(), bigKeyData.begin(), bigKeyData.size());

	try {
		co_await self->performTest(cx, data);
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			self->testFailure(fmt::format("Unhandled error {}: {}", e.code(), e.name()));
	}
}

Future<Void> ApiWorkload::start(Database const& cx) {
	if (clientId < maxClients || maxClients < 0)
		return ::start(cx, this);

	return Void();
}

// Convenience function for reporting a test failure to trace log and stdout
void ApiWorkload::testFailure(std::string reason) {
	fmt::println("test failure on client {}: {}", clientPrefixInt, reason);
	TraceEvent(SevError, "TestFailure")
	    .detail("Reason", description() + " " + reason)
	    .detail("Workload", "ApiCorrectness");
	success = false;
}

Future<bool> ApiWorkload::check(Database const& cx) {
	return success;
}

// Verifies that the results of a getRange are the same in the database and in memory
bool ApiWorkload::compareResults(VectorRef<KeyValueRef> dbResults,
                                 VectorRef<KeyValueRef> storeResults,
                                 Version readVersion) {
	if (dbResults.size() != storeResults.size()) {
		fmt::println("{}. Size mismatch: {} - {}", clientPrefixInt, dbResults.size(), storeResults.size());
		fmt::println("DB Range:");
		for (int j = 0; j < dbResults.size(); j++)
			fmt::println("{}: {} {}", j, dbResults[j].key.toString(), dbResults[j].value.size());

		fmt::println("Memory Range:");
		for (int j = 0; j < storeResults.size(); j++)
			fmt::println("{}: {} {}", j, storeResults[j].key.toString(), storeResults[j].value.size());

		fmt::println("Read Version: {}", readVersion);

		TraceEvent(SevError, fmt::format("{}_CompareSizeMismatch", description()).c_str())
		    .detail("ReadVer", readVersion)
		    .detail("ResultSize", dbResults.size())
		    .detail("StoreResultSize", storeResults.size());

		return false;
	}

	for (int i = 0; i < dbResults.size(); i++) {
		if (dbResults[i].key != storeResults[i].key || dbResults[i].value != storeResults[i].value) {
			fmt::println("{} mismatch at {}", dbResults[i].key != storeResults[i].key ? "Key" : "Value", i);
			fmt::println("DB Range:");
			for (int j = 0; j < dbResults.size(); j++)
				fmt::println("{}: {} {}", j, dbResults[j].key.toString(), dbResults[j].value.size());

			fmt::println("Memory Range:");
			for (int j = 0; j < storeResults.size(); j++)
				fmt::println("{}: {} {}", j, storeResults[j].key.toString(), storeResults[j].value.size());

			fmt::println("Read Version: {}", readVersion);

			TraceEvent(SevError, fmt::format("{}_CompareValueMismatch", description()).c_str())
			    .detail("ReadVer", readVersion)
			    .detail("ResultSize", dbResults.size())
			    .detail("DifferAt", i);

			return false;
		}
	}

	return true;
}

// Compares the contents of this client's key-space in the database with the in-memory key-value store
Future<bool> compareDatabaseToMemory(ApiWorkload* self) {
	Key startKey(self->clientPrefix);
	Key endKey(self->clientPrefix + "\xff");
	int resultsPerRange = 100;
	double startTime = now();

	while (true) {
		// Fetch a subset of the results from each of the database and the memory store and compare them
		RangeResult storeResults = self->store.getRange(KeyRangeRef(startKey, endKey), resultsPerRange, Reverse::False);

		Reference<TransactionWrapper> transaction = self->createTransaction();
		KeyRangeRef range(startKey, endKey);

		while (true) {
			Error err;
			try {
				RangeResult dbResults = co_await transaction->getRange(range, resultsPerRange, Reverse::False);

				// Compare results of database and memory store
				Version v = co_await transaction->getReadVersion();
				if (!self->compareResults(dbResults, storeResults, v)) {
					TraceEvent(SevError, "FailedComparisonToMemory").detail("StartTime", startTime);
					co_return false;
				}

				// If there are no more results, then return success
				if (storeResults.size() < resultsPerRange)
					co_return true;

				startKey = dbResults[dbResults.size() - 1].key;

				break;
			} catch (Error& e) {
				err = e;
			}
			co_await transaction->onError(err);
		}
	}
}

Future<bool> ApiWorkload::compareDatabaseToMemory() {
	return ::compareDatabaseToMemory(this);
}

// Generates a set of random key-value pairs with an optional prefix
Standalone<VectorRef<KeyValueRef>> ApiWorkload::generateData(int numKeys,
                                                             int minKeyLength,
                                                             int maxKeyLength,
                                                             int minValueLength,
                                                             int maxValueLength,
                                                             std::string prefix,
                                                             bool allowDuplicates) {
	Standalone<VectorRef<KeyValueRef>> data;
	std::set<KeyRef> keys;

	while (data.size() < numKeys) {
		Key key = generateKey(data, minKeyLength, maxKeyLength, prefix);

		if (!allowDuplicates && !keys.insert(key).second)
			continue;

		ValueRef value(data.arena(), generateValue(minValueLength, maxValueLength + 1));
		data.push_back_deep(data.arena(), KeyValueRef(key, value));
	}

	return data;
}

// Generates a random key
Key ApiWorkload::generateKey(VectorRef<KeyValueRef> const& data,
                             int minKeyLength,
                             int maxKeyLength,
                             std::string prefix) {
	int keyLength = deterministicRandom()->randomInt(minKeyLength, maxKeyLength + 1);
	char* keyBuffer = new char[keyLength + 1];

	if (onlyLowerCase) {
		for (int i = 0; i < keyLength; i++)
			keyBuffer[i] = deterministicRandom()->randomInt('a', 'z' + 1);
	} else {
		for (int i = 0; i < keyLength; i += sizeof(uint32_t)) {
			uint32_t val = deterministicRandom()->randomUInt32();
			memcpy(&keyBuffer[i], &val, std::min(keyLength - i, (int)sizeof(uint32_t)));
		}

		// Don't allow the first character of the key to be 0xff
		if (keyBuffer[0] == '\xff')
			keyBuffer[0] = deterministicRandom()->randomInt(0, 255);
	}

	// If encryption validation is enabled; slip "marker pattern" at random location in generated key
	if (g_network->isSimulated() && g_simulator->dataAtRestPlaintextMarker.present() &&
	    keyLength + 1 > g_simulator->dataAtRestPlaintextMarker.get().size()) {
		int len = keyLength - g_simulator->dataAtRestPlaintextMarker.get().size();
		// Avoid updating the first byte of the key
		int idx = len > 1 ? deterministicRandom()->randomInt(1, len) : 1;
		memcpy(&keyBuffer[idx],
		       g_simulator->dataAtRestPlaintextMarker.get().c_str(),
		       g_simulator->dataAtRestPlaintextMarker.get().size());
		//TraceEvent(SevDebug, "ModifiedKey").suppressFor(5).detail("Key", keyBuffer);
	}

	keyBuffer[keyLength] = '\0';

	Key key(prefix + keyBuffer);
	delete[] keyBuffer;

	return key;
}

// Generates a random key selector with a specified maximum offset
KeySelector ApiWorkload::generateKeySelector(VectorRef<KeyValueRef> const& data, int maxOffset) {
	Key key = selectRandomKey(data, 0.5);
	return KeySelector(KeySelectorRef(
	    key, deterministicRandom()->randomInt(0, 2) == 1, deterministicRandom()->randomInt(-maxOffset, maxOffset + 1)));
}

// Selects a random key.  There is a <probabilityKeyExists> probability that the key will be chosen from the keyset in
// data, otherwise the key will be a randomly generated key
Key ApiWorkload::selectRandomKey(VectorRef<KeyValueRef> const& data, double probabilityKeyExists) {
	if (deterministicRandom()->random01() < probabilityKeyExists)
		return data[deterministicRandom()->randomInt(0, data.size())].key;
	else
		return generateKey(data, minLongKeyLength, maxLongKeyLength, clientPrefix);
}

// Generates a random value
Value ApiWorkload::generateValue(int minValueLength, int maxValueLength) {
	int valueLength = deterministicRandom()->randomInt(minValueLength, maxValueLength + 1);
	std::string ret(std::string(valueLength, 'x'));

	// If encryption validation is enabled; slip "marker pattern" at random location in generated key
	if (g_network->isSimulated() && g_simulator->dataAtRestPlaintextMarker.present() &&
	    valueLength > g_simulator->dataAtRestPlaintextMarker.get().size()) {
		int len = valueLength - g_simulator->dataAtRestPlaintextMarker.get().size();
		int idx = deterministicRandom()->randomInt(0, len);
		memcpy(&ret[idx],
		       g_simulator->dataAtRestPlaintextMarker.get().c_str(),
		       g_simulator->dataAtRestPlaintextMarker.get().size());
		//TraceEvent("ModifiedValue").suppressFor(5).detail("Value", ret);
	}
	return Value(ret);
}

// Generates a random value
Value ApiWorkload::generateValue() {
	return generateValue(minValueLength, maxValueLength);
}

// Creates a random transaction factory to produce transaction of one of the TransactionType choices
Future<Void> chooseTransactionFactory(Database cx, std::vector<TransactionType> choices, ApiWorkload* self) {
	TransactionType transactionType = deterministicRandom()->randomChoice(choices);
	self->transactionType = transactionType;

	if (transactionType == NATIVE) {
		fmt::println("client {}: Running NativeAPI Transactions", self->clientPrefixInt);
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<FlowTransactionWrapper<Transaction>, const Database>(
		        cx, self->extraDB, self->useExtraDB));
	} else if (transactionType == READ_YOUR_WRITES) {
		fmt::println("client {}: Running ReadYourWrites Transactions", self->clientPrefixInt);
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<FlowTransactionWrapper<ReadYourWritesTransaction>, const Database>(
		        cx, self->extraDB, self->useExtraDB));
	} else if (transactionType == THREAD_SAFE) {
		fmt::println("client {}: Running ThreadSafe Transactions", self->clientPrefixInt);
		Reference<IDatabase> dbHandle =
		    co_await unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx));
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<ThreadTransactionWrapper, Reference<IDatabase>>(dbHandle, dbHandle, false));
	} else if (transactionType == MULTI_VERSION) {
		fmt::println("client {}: Running Multi-Version Transactions", self->clientPrefixInt);
		MultiVersionApi::api->selectApiVersion(cx->apiVersion.version());
		Reference<IDatabase> threadSafeHandle =
		    co_await unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx));
		Reference<IDatabase> dbHandle = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<ThreadTransactionWrapper, Reference<IDatabase>>(dbHandle, dbHandle, false));
	}
}

Future<Void> ApiWorkload::chooseTransactionFactory(Database const& cx, std::vector<TransactionType> const& choices) {
	return ::chooseTransactionFactory(cx, choices, this);
}

// Creates a new transaction using the current transaction factory
Reference<TransactionWrapper> ApiWorkload::createTransaction() {
	ASSERT(transactionFactory);
	return transactionFactory->createTransaction();
}

bool ApiWorkload::hasFailed() const {
	return !success;
}
