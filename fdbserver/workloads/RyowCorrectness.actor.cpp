/*
 * RyowCorrectness.actor.cpp
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

#include <vector>

#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/MemoryKeyValueStore.h"
#include "fdbserver/workloads/ApiWorkload.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define TRACE_TRANSACTION 0

#if TRACE_TRANSACTION
StringRef debugKey = "0000000000uldlamzpspf"_sr;
#endif

// A struct representing an operation to be performed on a transaction
struct Operation {

	// An enum of API operation types to perform in a transaction
	enum OperationType { SET, GET, GET_RANGE, GET_RANGE_SELECTOR, GET_KEY, CLEAR, CLEAR_RANGE };

	OperationType type;

	Key beginKey;
	Key endKey;

	KeySelector beginSelector;
	KeySelector endSelector;

	Value value;

	int limit;
	Reverse reverse{ Reverse::False };
};

// A workload which executes random sequences of operations on RYOW transactions and confirms the results
struct RyowCorrectnessWorkload : ApiWorkload {
	static constexpr auto NAME = "RyowCorrectness";

	// How long the test should run
	int duration;

	// The number of operations to perform on a transaction in between commits
	int opsPerTransaction;

	RyowCorrectnessWorkload(WorkloadContext const& wcx) : ApiWorkload(wcx, 1) {
		duration = getOption(options, "duration"_sr, 60);
		opsPerTransaction = getOption(options, "opsPerTransaction"_sr, 50);
	}

	ACTOR Future<Void> performSetup(Database cx, RyowCorrectnessWorkload* self) {
		std::vector<TransactionType> types;
		types.push_back(READ_YOUR_WRITES);

		wait(self->chooseTransactionFactory(cx, types));
		return Void();
	}

	Future<Void> performSetup(Database const& cx) override { return performSetup(cx, this); }

	// Generates a random sequence of operations to perform in a single transaction
	std::vector<Operation> generateOperationSequence(Standalone<VectorRef<KeyValueRef>> const& data) {
		std::vector<Operation> sequence;

		int pdfArray[] = { 0, 100, 100, 50, 50, 20, 100, 5 };
		std::vector<int> pdf = std::vector<int>(pdfArray, pdfArray + 8);

		// Choose a random operation type (SET, GET, GET_RANGE, GET_RANGE_SELECTOR, GET_KEY, CLEAR, CLEAR_RANGE).
		int totalDensity = 0;
		for (int i = 0; i < pdf.size(); i++)
			totalDensity += pdf[i];

		for (int i = 0; i < opsPerTransaction; ++i) {
			int cumulativeDensity = 0;
			int random = deterministicRandom()->randomInt(0, totalDensity);
			for (int i = 0; i < pdf.size() - 1; i++) {
				if (cumulativeDensity + pdf[i] <= random && random < cumulativeDensity + pdf[i] + pdf[i + 1]) {
					Operation info;
					info.type = (Operation::OperationType)i;

					switch (info.type) {
					case Operation::GET:
					case Operation::CLEAR:
						info.beginKey = selectRandomKey(data, .8);
						break;
					case Operation::SET:
						info.beginKey = selectRandomKey(data, .5);
						info.value = generateValue();
						break;
					case Operation::GET_RANGE:
					case Operation::CLEAR_RANGE:
						info.beginKey = selectRandomKey(data, .8);
						info.endKey = selectRandomKey(data, .8);
						info.limit = deterministicRandom()->randomInt(0, 1000);
						info.reverse.set(deterministicRandom()->coinflip());

						if (info.beginKey > info.endKey)
							std::swap(info.beginKey, info.endKey);

						break;
					case Operation::GET_RANGE_SELECTOR:
					case Operation::GET_KEY:
						info.beginSelector = generateKeySelector(data, 1000);
						info.endSelector = generateKeySelector(data, 1000);
						info.limit = deterministicRandom()->randomInt(0, 1000);
						info.reverse.set(deterministicRandom()->coinflip());
						break;
					}

					sequence.push_back(info);
					break;
				}

				cumulativeDensity += pdf[i];
			}
		}

		return sequence;
	}

	// Adds a single KV-pair to the list of results
	void pushKVPair(std::vector<RangeResult>& results, Key const& key, Optional<Value> const& value) {
		RangeResult result;
		if (!value.present())
			result.push_back_deep(result.arena(), KeyValueRef(key, "VALUE_NOT_PRESENT"_sr));
		else
			result.push_back_deep(result.arena(), KeyValueRef(key, value.get()));

		results.push_back(result);
	}

	// Applies a sequence of operations to the memory store and returns the results
	std::vector<RangeResult> applySequenceToStore(std::vector<Operation> sequence) {
		std::vector<RangeResult> results;
		Key key;

#if TRACE_TRANSACTION
		printf("NEW_TRANSACTION\n");
#endif

		for (auto op : sequence) {
			switch (op.type) {
			case Operation::SET:
				store.set(op.beginKey, op.value);
#if TRACE_TRANSACTION
				if (op.beginKey == debugKey)
					printf("SET: %s = %d\n", printable(op.beginKey).c_str(), op.value.size());
#endif
				break;
			case Operation::GET:
				pushKVPair(results, op.beginKey, store.get(op.beginKey));
#if TRACE_TRANSACTION && 0
				if (op.beginKey == debugKey)
					printf("GET: %s\n", printable(op.beginKey).c_str());
#endif
				break;
			case Operation::GET_RANGE_SELECTOR:
				op.beginKey = store.getKey(op.beginSelector);
				op.endKey = store.getKey(op.endSelector);

				if (op.beginKey > op.endKey)
					op.endKey = op.beginKey;
				// Fall-through
			case Operation::GET_RANGE:
				results.push_back(store.getRange(KeyRangeRef(op.beginKey, op.endKey), op.limit, op.reverse));
#if TRACE_TRANSACTION
				if (op.beginKey <= debugKey && debugKey < op.endKey)
					printf("%s: %s - %s (limit=%d, reverse=%d)\n",
					       op.type == Operation::GET_RANGE ? "GET_RANGE" : "GET_RANGE_SELECTOR",
					       printable(op.beginKey).c_str(),
					       printable(op.endKey).c_str(),
					       op.limit,
					       op.reverse);
#endif
				break;
			case Operation::GET_KEY:
				key = store.getKey(op.beginSelector);
				pushKVPair(results, key, Value());
#if TRACE_TRANSACTION
				if (key == debugKey)
					printf("GET_KEY: %s = %s\n", op.beginSelector.toString().c_str(), printable(key).c_str());
#endif
				break;
			case Operation::CLEAR:
				store.clear(op.beginKey);
#if TRACE_TRANSACTION
				if (op.beginKey == debugKey)
					printf("CLEAR: %s\n", printable(op.beginKey).c_str());
#endif
				break;
			case Operation::CLEAR_RANGE:
				store.clear(KeyRangeRef(op.beginKey, op.endKey));
#if TRACE_TRANSACTION
				if (op.beginKey <= debugKey && debugKey < op.endKey)
					printf("CLEAR_RANGE: %s - %s\n", printable(op.beginKey).c_str(), printable(op.endKey).c_str());
#endif
				break;
			}
		}

		return results;
	}

	// Applies a sequence of operations to the database and returns the results
	ACTOR Future<std::vector<RangeResult>> applySequenceToDatabase(Reference<TransactionWrapper> transaction,
	                                                               std::vector<Operation> sequence,
	                                                               RyowCorrectnessWorkload* self) {
		state bool dontUpdateResults = false;
		state std::vector<RangeResult> results;
		loop {
			try {
				state int i;
				for (i = 0; i < sequence.size(); ++i) {
					state Operation op = sequence[i];

					if (op.type == Operation::SET) {
						transaction->set(op.beginKey, op.value);
					} else if (op.type == Operation::GET) {
						Optional<Value> val = wait(transaction->get(op.beginKey));
						if (!dontUpdateResults)
							self->pushKVPair(results, op.beginKey, val);
					} else if (op.type == Operation::GET_RANGE) {
						KeyRangeRef range(op.beginKey, op.endKey);
						RangeResult result = wait(transaction->getRange(range, op.limit, op.reverse));
						if (!dontUpdateResults)
							results.push_back((RangeResultRef)result);
					} else if (op.type == Operation::GET_RANGE_SELECTOR) {
						RangeResult result =
						    wait(transaction->getRange(op.beginSelector, op.endSelector, op.limit, op.reverse));
						if (!dontUpdateResults)
							results.push_back((RangeResultRef)result);
					} else if (op.type == Operation::GET_KEY) {
						Key key = wait(transaction->getKey(op.beginSelector));
						if (!dontUpdateResults)
							self->pushKVPair(results, key, Value());
					} else if (op.type == Operation::CLEAR) {
						transaction->clear(op.beginKey);
					} else if (op.type == Operation::CLEAR_RANGE) {
						KeyRangeRef range(op.beginKey, op.endKey);
						transaction->clear(range);
					}
				}

				wait(transaction->commit());
				return results;
			} catch (Error& e) {
				// If the transaction was possibly committed, then keep the results that we got (since they might change
				// the next time around the loop), but try to commit the transaction again
				if (e.code() == error_code_commit_unknown_result)
					dontUpdateResults = true;
				else if (!dontUpdateResults)
					results.clear();

				wait(transaction->onError(e));
			}
		}
	}

	// Compares a sequence of results from the database and the memory store
	bool compareResults(std::vector<RangeResult> dbResults,
	                    std::vector<RangeResult> storeResults,
	                    std::vector<Operation> sequence,
	                    Version readVersion) {
		ASSERT(storeResults.size() == dbResults.size());

		int currentResult = 0;
		for (int i = 0; i < sequence.size(); ++i) {
			Operation op = sequence[i];
			if (op.type == Operation::SET || op.type == Operation::CLEAR || op.type == Operation::CLEAR_RANGE)
				continue;

			if (!ApiWorkload::compareResults(dbResults[currentResult], storeResults[currentResult], readVersion)) {
				switch (op.type) {
				case Operation::GET:
					printf("Operation GET failed: key = %s\n", printable(op.beginKey).c_str());
					break;
				case Operation::GET_RANGE:
					printf("Operation GET_RANGE failed: begin = %s, end = %s, limit = %d, reverse = %d\n",
					       printable(op.beginKey).c_str(),
					       printable(op.endKey).c_str(),
					       op.limit,
					       static_cast<bool>(op.reverse));
					break;
				case Operation::GET_RANGE_SELECTOR:
					printf("Operation GET_RANGE_SELECTOR failed: begin = %s, end = %s, limit = %d, reverse = %d\n",
					       op.beginSelector.toString().c_str(),
					       op.endSelector.toString().c_str(),
					       op.limit,
					       static_cast<bool>(op.reverse));
					break;
				case Operation::GET_KEY:
					printf("Operation GET_KEY failed: selector = %s\n", op.beginSelector.toString().c_str());
					break;
				default:
					break;
				}

				return false;
			}

			++currentResult;
		}

		ASSERT(currentResult == storeResults.size());

		return true;
	}

	// Execute transactions with multiple random operations each
	ACTOR Future<Void> performTest(Database cx,
	                               Standalone<VectorRef<KeyValueRef>> data,
	                               RyowCorrectnessWorkload* self) {
		loop {
			state Reference<TransactionWrapper> transaction = self->createTransaction();
			state std::vector<Operation> sequence = self->generateOperationSequence(data);
			state std::vector<RangeResult> storeResults = self->applySequenceToStore(sequence);
			state std::vector<RangeResult> dbResults = wait(self->applySequenceToDatabase(transaction, sequence, self));

			Version readVersion = wait(transaction->getReadVersion());

			state bool result = self->compareResults(dbResults, storeResults, sequence, readVersion);
			if (!result)
				self->testFailure("Transaction results did not match");

			bool result2 = wait(self->compareDatabaseToMemory());
			if (result && !result2)
				self->testFailure("Database contents did not match");

			if (!result || !result2)
				return Void();
		}
	}

	Future<Void> performTest(Database const& cx, Standalone<VectorRef<KeyValueRef>> const& data) override {
		return ::success(timeout(performTest(cx, data, this), duration));
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RyowCorrectnessWorkload> RyowCorrectnessWorkloadFactory;
