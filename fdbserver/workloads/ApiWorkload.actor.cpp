/*
 * ApiWorkload.actor.cpp
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

#include <cinttypes>
#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbserver/workloads/ApiWorkload.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Clears the keyspace used by this test
ACTOR Future<Void> clearKeyspace(ApiWorkload* self) {
	loop {
		state Reference<TransactionWrapper> transaction = self->createTransaction();
		try {
			KeyRange range(KeyRangeRef(StringRef(format("%010d", self->clientPrefixInt)),
			                           StringRef(format("%010d", self->clientPrefixInt + 1))));

			transaction->clear(range);
			wait(transaction->commit());
			return Void();
		} catch (Error& e) {
			wait(transaction->onError(e));
		}
	}
}

Future<Void> ApiWorkload::clearKeyspace() {
	return ::clearKeyspace(this);
}

ACTOR Future<Void> setup(Database cx, ApiWorkload* self) {
	self->transactionFactory = Reference<TransactionFactoryInterface>(
	    new TransactionFactory<FlowTransactionWrapper<Transaction>, const Database>(cx, cx, false));

	// Clear keyspace before running
	wait(timeoutError(self->clearKeyspace(), 600));
	wait(self->performSetup(cx));

	return Void();
}

Future<Void> ApiWorkload::setup(Database const& cx) {
	if (clientId < maxClients || maxClients < 0)
		return ::setup(cx, this);

	return Void();
}

ACTOR Future<Void> start(Database cx, ApiWorkload* self) {
	// Generate the data to store in this client's key-space
	state Standalone<VectorRef<KeyValueRef>> data = self->generateData(self->numKeys * self->shortKeysRatio,
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
		wait(self->performTest(cx, data));
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			self->testFailure(format("Unhandled error %d: %s", e.code(), e.name()));
	}

	return Void();
}

Future<Void> ApiWorkload::start(Database const& cx) {
	if (clientId < maxClients || maxClients < 0)
		return ::start(cx, this);

	return Void();
}

// Convenience function for reporting a test failure to trace log and stdout
void ApiWorkload::testFailure(std::string reason) {
	printf("test failure on client %d: %s\n", clientPrefixInt, reason.c_str());
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
		printf("%d. Size mismatch: %d - %d\n", clientPrefixInt, dbResults.size(), storeResults.size());
		printf("DB Range:\n");
		for (int j = 0; j < dbResults.size(); j++)
			printf("%d: %s %d\n", j, dbResults[j].key.toString().c_str(), dbResults[j].value.size());

		printf("Memory Range:\n");
		for (int j = 0; j < storeResults.size(); j++)
			printf("%d: %s %d\n", j, storeResults[j].key.toString().c_str(), storeResults[j].value.size());

		fmt::print("Read Version: {}\n", readVersion);

		TraceEvent(SevError, format("%s_CompareSizeMismatch", description().c_str()).c_str())
		    .detail("ReadVer", readVersion)
		    .detail("ResultSize", dbResults.size())
		    .detail("StoreResultSize", storeResults.size());

		return false;
	}

	for (int i = 0; i < dbResults.size(); i++) {
		if (dbResults[i].key != storeResults[i].key || dbResults[i].value != storeResults[i].value) {
			printf("%s mismatch at %d\n", dbResults[i].key != storeResults[i].key ? "Key" : "Value", i);
			printf("DB Range:\n");
			for (int j = 0; j < dbResults.size(); j++)
				printf("%d: %s %d\n", j, dbResults[j].key.toString().c_str(), dbResults[j].value.size());

			printf("Memory Range:\n");
			for (int j = 0; j < storeResults.size(); j++)
				printf("%d: %s %d\n", j, storeResults[j].key.toString().c_str(), storeResults[j].value.size());

			fmt::print("Read Version: {}\n", readVersion);

			TraceEvent(SevError, format("%s_CompareValueMismatch", description().c_str()).c_str())
			    .detail("ReadVer", readVersion)
			    .detail("ResultSize", dbResults.size())
			    .detail("DifferAt", i);

			return false;
		}
	}

	return true;
}

// Compares the contents of this client's key-space in the database with the in-memory key-value store
ACTOR Future<bool> compareDatabaseToMemory(ApiWorkload* self) {
	state Key startKey(self->clientPrefix);
	state Key endKey(self->clientPrefix + "\xff");
	state int resultsPerRange = 100;
	state double startTime = now();

	loop {
		// Fetch a subset of the results from each of the database and the memory store and compare them
		state RangeResult storeResults =
		    self->store.getRange(KeyRangeRef(startKey, endKey), resultsPerRange, Reverse::False);

		state Reference<TransactionWrapper> transaction = self->createTransaction();
		state KeyRangeRef range(startKey, endKey);

		loop {
			try {
				state RangeResult dbResults = wait(transaction->getRange(range, resultsPerRange, Reverse::False));

				// Compare results of database and memory store
				Version v = wait(transaction->getReadVersion());
				if (!self->compareResults(dbResults, storeResults, v)) {
					TraceEvent(SevError, "FailedComparisonToMemory").detail("StartTime", startTime);
					return false;
				}

				// If there are no more results, then return success
				if (storeResults.size() < resultsPerRange)
					return true;

				startKey = dbResults[dbResults.size() - 1].key;

				break;
			} catch (Error& e) {
				wait(transaction->onError(e));
			}
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
	return Value(std::string(valueLength, 'x'));
}

// Generates a random value
Value ApiWorkload::generateValue() {
	return generateValue(minValueLength, maxValueLength);
}

// Creates a random transaction factory to produce transaction of one of the TransactionType choices
ACTOR Future<Void> chooseTransactionFactory(Database cx, std::vector<TransactionType> choices, ApiWorkload* self) {
	TransactionType transactionType = deterministicRandom()->randomChoice(choices);
	self->transactionType = transactionType;

	if (transactionType == NATIVE) {
		printf("client %d: Running NativeAPI Transactions\n", self->clientPrefixInt);
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<FlowTransactionWrapper<Transaction>, const Database>(
		        cx, self->extraDB, self->useExtraDB));
	} else if (transactionType == READ_YOUR_WRITES) {
		printf("client %d: Running ReadYourWrites Transactions\n", self->clientPrefixInt);
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<FlowTransactionWrapper<ReadYourWritesTransaction>, const Database>(
		        cx, self->extraDB, self->useExtraDB));
	} else if (transactionType == THREAD_SAFE) {
		printf("client %d: Running ThreadSafe Transactions\n", self->clientPrefixInt);
		Reference<IDatabase> dbHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<ThreadTransactionWrapper, Reference<IDatabase>>(dbHandle, dbHandle, false));
	} else if (transactionType == MULTI_VERSION) {
		printf("client %d: Running Multi-Version Transactions\n", self->clientPrefixInt);
		MultiVersionApi::api->selectApiVersion(cx->apiVersion);
		Reference<IDatabase> threadSafeHandle =
		    wait(unsafeThreadFutureToFuture(ThreadSafeDatabase::createFromExistingDatabase(cx)));
		Reference<IDatabase> dbHandle = MultiVersionDatabase::debugCreateFromExistingDatabase(threadSafeHandle);
		self->transactionFactory = Reference<TransactionFactoryInterface>(
		    new TransactionFactory<ThreadTransactionWrapper, Reference<IDatabase>>(dbHandle, dbHandle, false));
	}

	return Void();
}

Future<Void> ApiWorkload::chooseTransactionFactory(Database const& cx, std::vector<TransactionType> const& choices) {
	return ::chooseTransactionFactory(cx, choices, this);
}

// Creates a new transaction using the current transaction factory
Reference<TransactionWrapper> ApiWorkload::createTransaction() {
	ASSERT(transactionFactory);
	return transactionFactory->createTransaction();
}

bool ApiWorkload::hasFailed() {
	return !success;
}
