/*
 * TesterAtomicOpsCorrectnessWorkload.cpp
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
#include "TesterApiWorkload.h"
#include "TesterUtil.h"
#include "fdb_c_options.g.h"
#include "fmt/core.h"
#include "test/fdb_api.hpp"
#include <cctype>
#include <memory>
#include <fmt/format.h>

namespace FdbApiTester {

using fdb::Key;
using fdb::Value;
using fdb::ValueRef;

class AtomicOpsCorrectnessWorkload : public ApiWorkload {
public:
	AtomicOpsCorrectnessWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	typedef std::function<uint64_t(uint64_t, uint64_t)> IntAtomicOpFunction;
	typedef std::function<Value(ValueRef, ValueRef)> AtomicOpFunction;

	enum OpType {
		OP_ATOMIC_ADD,
		OP_ATOMIC_BIT_AND,
		OP_ATOMIC_BIT_OR,
		OP_ATOMIC_BIT_XOR,
		OP_ATOMIC_APPEND_IF_FITS,
		OP_ATOMIC_MAX,
		OP_ATOMIC_MIN,
		OP_ATOMIC_VERSIONSTAMPED_KEY,
		OP_ATOMIC_VERSIONSTAMPED_VALUE,
		OP_ATOMIC_BYTE_MIN,
		OP_ATOMIC_BYTE_MAX,
		OP_ATOMIC_COMPARE_AND_CLEAR,
		OP_LAST = OP_ATOMIC_COMPARE_AND_CLEAR
	};

	static uint64_t toUint64_t(fdb::BytesRef value) {
		ASSERT(value.size() == sizeof(uint64_t));
		uint64_t output;
		memcpy(&output, value.data(), value.size());
		return output;
	}

	template <typename T>
	static fdb::ByteString toByteString(T value) {
		fdb::ByteString output(sizeof(T), 0);
		memcpy(output.data(), (const uint8_t*)&value, sizeof(value));
		return output;
	}

	void randomOperation(TTaskFct cont) override {
		OpType txType = (OpType)Random::get().randomInt(0, OP_LAST);

		switch (txType) {
		case OP_ATOMIC_ADD:
			testIntAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_ADD, [](uint64_t val1, uint64_t val2) { return val1 + val2; }, cont);
			break;
		case OP_ATOMIC_BIT_AND:
			testIntAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_BIT_AND,
			    [](uint64_t val1, uint64_t val2) { return val1 & val2; },
			    cont);
			break;
		case OP_ATOMIC_BIT_OR:
			testIntAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_BIT_OR,
			    [](uint64_t val1, uint64_t val2) { return val1 | val2; },
			    cont);
			break;
		case OP_ATOMIC_BIT_XOR:
			testIntAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_BIT_XOR,
			    [](uint64_t val1, uint64_t val2) { return val1 ^ val2; },
			    cont);
			break;
		case OP_ATOMIC_APPEND_IF_FITS: {
			Value val1 = randomValue();
			Value val2 = randomValue();
			testAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_APPEND_IF_FITS,
			    val1,
			    val2,
			    [](ValueRef val1, ValueRef val2) { return Value(val1) + Value(val2); },
			    cont);
			break;
		}
		case OP_ATOMIC_MAX:
			testIntAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_MAX,
			    [](uint64_t val1, uint64_t val2) { return std::max(val1, val2); },
			    cont);
			break;
		case OP_ATOMIC_MIN:
			testIntAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_MIN,
			    [](uint64_t val1, uint64_t val2) { return std::min(val1, val2); },
			    cont);
			break;
		case OP_ATOMIC_VERSIONSTAMPED_KEY:
			testAtomicVersionstampedKeyOp(cont);
			break;
		case OP_ATOMIC_VERSIONSTAMPED_VALUE:
			testAtomicVersionstampedValueOp(cont);
			break;
		case OP_ATOMIC_BYTE_MIN: {
			Value val1 = randomValue();
			Value val2 = randomValue();
			testAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_BYTE_MIN,
			    val1,
			    val2,
			    [](ValueRef val1, ValueRef val2) { return Value(std::min(val1, val2)); },
			    cont);
			break;
		}
		case OP_ATOMIC_BYTE_MAX: {
			Value val1 = randomValue();
			Value val2 = randomValue();
			testAtomicOp(
			    FDBMutationType::FDB_MUTATION_TYPE_BYTE_MAX,
			    val1,
			    val2,
			    [](ValueRef val1, ValueRef val2) { return Value(std::max(val1, val2)); },
			    cont);
			break;
		}
		case OP_ATOMIC_COMPARE_AND_CLEAR:
			testAtomicCompareAndClearOp(cont);
			break;
		}
	}

	Key getKeySuffix() { return Key(fdb::toBytesRef(fmt::format("_{}_{}", clientId, atomicOpId++))); }

	void testIntAtomicOp(FDBMutationType opType, IntAtomicOpFunction opFunc, TTaskFct cont) {
		uint64_t intValue1 = Random::get().randomInt(0, 10000000);
		uint64_t intValue2 = Random::get().randomInt(0, 10000000);

		Value val1 = toByteString(intValue1);
		Value val2 = toByteString(intValue2);
		testAtomicOp(
		    opType,
		    val1,
		    val2,
		    [opFunc](ValueRef val1, ValueRef val2) { return toByteString(opFunc(toUint64_t(val1), toUint64_t(val2))); },
		    cont);
	}

	void testAtomicOp(FDBMutationType opType, Value val1, Value val2, AtomicOpFunction opFunc, TTaskFct cont) {
		Key key(randomKeyName() + getKeySuffix());
		execTransaction(
		    // 1. Set the key to val1
		    [key, val1](auto ctx) {
			    ctx->tx().set(key, val1);
			    ctx->commit();
		    },
		    [this, opType, opFunc, key, val1, val2, cont]() {
			    execTransaction(
			        // 2. Perform the given atomic operation to val2, but only if it hasn't been applied yet, otherwise
			        // retries of commit_unknown_result would cause the operation to be applied multiple times, see
			        // https://github.com/apple/foundationdb/issues/1321.
			        [key, opType, val1, val2](auto ctx) {
				        auto f = ctx->tx().get(key, false);
				        ctx->continueAfter(f, [ctx, f, opType, key, val1, val2]() {
					        auto outputVal = f.get();
					        ASSERT(outputVal.has_value());
					        if (outputVal.value() == val1) {
						        ctx->tx().atomicOp(key, val2, opType);
						        ctx->commit();
					        } else {
						        ctx->done();
					        }
				        });
			        },
			        [this, opFunc, key, val1, val2, cont]() {
				        auto result = std::make_shared<Value>();

				        execTransaction(
				            // 3. Fetch the final value.
				            [key, result](auto ctx) {
					            auto f = ctx->tx().get(key, false);
					            ctx->continueAfter(
					                f,
					                [ctx, f, result]() {
						                auto outputVal = f.get();
						                ASSERT(outputVal.has_value());
						                *result = outputVal.value();
						                ctx->done();
					                },
					                true);
				            },
				            [this, opFunc, key, val1, val2, result, cont]() {
					            // 4. Assert expectation.
					            auto expected = opFunc(val1, val2);
					            if (*result != expected) {
						            error(fmt::format("testAtomicOp expected: {} actual: {}",
						                              fdb::toCharsRef(expected),
						                              fdb::toCharsRef(*result)));
						            ASSERT(false);
					            }
					            schedule(cont);
				            });
			        });
		    });
	}

	void testAtomicVersionstampedKeyOp(TTaskFct cont) {
		Key keyPrefix(randomKeyName() + getKeySuffix());
		Key key = keyPrefix + fdb::ByteString(10, '\0') + toByteString((uint32_t)keyPrefix.size());
		Value val = randomValue();

		auto versionstamp = std::make_shared<Key>();
		execTransaction(
		    // 1. Perform SetVersionstampedKey operation.
		    [key, val, versionstamp](auto ctx) {
			    ctx->tx().atomicOp(key, val, FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);
			    // TODO: Figure out how to correctly retrieve version timestamps.
			    // See testAtomicVersionstampedValueOp() for detailed discussion.
			    ctx->commit();
		    },
		    [this, keyPrefix, val, versionstamp, cont]() {
			    auto resultKey = std::make_shared<Key>();
			    auto resultVal = std::make_shared<Value>();
			    execTransaction(
			        // 2. Fetch the resulting versionstamped key and value.
			        [keyPrefix, resultKey, resultVal](auto ctx) {
				        auto f = ctx->tx().getKey(fdb::key_select::firstGreaterOrEqual(keyPrefix), false);
				        ctx->continueAfter(
				            f,
				            [ctx, f, resultKey, resultVal]() {
					            *resultKey = f.get();
					            auto fv = ctx->tx().get(*resultKey, false);
					            ctx->continueAfter(fv, [ctx, fv, resultVal]() {
						            auto outputVal = fv.get();
						            ASSERT(outputVal.has_value());
						            *resultVal = outputVal.value();

						            ctx->done();
					            });
				            },
				            true);
			        },
			        [this, keyPrefix, val, resultKey, resultVal, versionstamp, cont]() {
				        // 3. Assert expectation.
				        // ASSERT(*resultKey == keyPrefix + *versionstamp);
				        // TODO: Enable the above assert instead of the ones below once versionstamp
				        // is correctly retrieved.
				        ASSERT(resultKey->substr(0, keyPrefix.size()) == keyPrefix);
				        ASSERT(toUint64_t(resultKey->substr(keyPrefix.size(), 8)) > 0);
				        ASSERT(*resultVal == val);
				        schedule(cont);
			        });
		    });
	}

	void testAtomicVersionstampedValueOp(TTaskFct cont) {
		Key key(randomKeyName() + getKeySuffix());
		Value valPrefix = randomValue();
		Value val = valPrefix + fdb::ByteString(10, '\0') + toByteString((uint32_t)valPrefix.size());
		auto versionstamp = std::make_shared<Key>();
		execTransaction(
		    // 1. Perform SetVersionstampedValue operation.
		    [key, val, versionstamp](auto ctx) {
			    ctx->tx().atomicOp(key, val, FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);
			    // TODO: Figure out how to correctly retrieve version timestamps. This may require
			    // changes to TesterTransactionExectutor.
			    // The following works most of the time, but fails when the transaction returns
			    // commit_unknown_result, and the TransactionExecutor automatically retries it.
			    // In that case, future f fails with transaction_invalid_version which in turn
			    // fails the whole transaction.

			    // auto f = ctx->tx().getVersionstamp();
			    // ctx->continueAfter(f, [f, versionstamp]() { *versionstamp = Key(f.get()); });
			    ctx->commit();
		    },
		    [this, key, valPrefix, versionstamp, cont]() {
			    auto result = std::make_shared<Value>();

			    execTransaction(
			        // 2. Fetch the resulting versionstamped value.
			        [key, result](auto ctx) {
				        auto f = ctx->tx().get(key, false);
				        ctx->continueAfter(
				            f,
				            [ctx, f, result]() {
					            auto outputVal = f.get();
					            ASSERT(outputVal.has_value());
					            *result = outputVal.value();
					            ctx->done();
				            },
				            true);
			        },
			        [this, key, valPrefix, result, versionstamp, cont]() {
				        // 3. Assert expectation.
				        // ASSERT(*result == valPrefix + *versionstamp);
				        // TODO: Enable the above assert instead of the ones below once versionstamp
				        // is correctly retrieved.
				        ASSERT(result->substr(0, valPrefix.size()) == valPrefix);
				        ASSERT(toUint64_t(result->substr(valPrefix.size(), 8)) > 0);
				        schedule(cont);
			        });
		    });
	}

	void testAtomicCompareAndClearOp(TTaskFct cont) {
		Key key(randomKeyName() + getKeySuffix());
		Value val = randomValue();
		execTransaction(
		    // 1. Set the key to initial value
		    [key, val](auto ctx) {
			    ctx->tx().set(key, val);
			    ctx->commit();
		    },
		    [this, key, val, cont]() {
			    execTransaction(
			        // 2. Perform CompareAndClear operation.
			        [key, val](auto ctx) {
				        ctx->tx().atomicOp(key, val, FDBMutationType::FDB_MUTATION_TYPE_COMPARE_AND_CLEAR);
				        ctx->commit();
			        },
			        [this, key, cont]() {
				        execTransaction(
				            // 3. Verify that the key was cleared.
				            [key](auto ctx) {
					            auto f = ctx->tx().get(key, false);
					            ctx->continueAfter(
					                f,
					                [ctx, f]() {
						                auto outputVal = f.get();
						                ASSERT(!outputVal.has_value());
						                ctx->done();
					                },
					                true);
				            },
				            [this, cont]() { schedule(cont); });
			        });
		    });
	}

	int atomicOpId = 0;
};

WorkloadFactory<AtomicOpsCorrectnessWorkload> AtomicOpsCorrectnessWorkloadFactory("AtomicOpsCorrectness");

} // namespace FdbApiTester
