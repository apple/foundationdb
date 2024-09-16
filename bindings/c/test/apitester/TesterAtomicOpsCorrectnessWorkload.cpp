/*
 * TesterAtomicOpsCorrectnessWorkload.cpp
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

	void testIntAtomicOp(FDBMutationType opType, IntAtomicOpFunction opFunc, TTaskFct cont) {
		uint64_t intValue1 = Random::get().randomInt(0, 10000000);
		uint64_t intValue2 = Random::get().randomInt(0, 10000000);

		Value val1 = toByteString(intValue1);
		Value val2 = toByteString(intValue2);
		testAtomicOp(
		    opType,
		    val1,
		    val2,
		    [opFunc](ValueRef val1, ValueRef val2) {
			    return toByteString(opFunc(toInteger<uint64_t>(val1), toInteger<uint64_t>(val2)));
		    },
		    cont);
	}

	void testAtomicOp(FDBMutationType opType, Value val1, Value val2, AtomicOpFunction opFunc, TTaskFct cont) {
		Key key(randomKeyName());
		execTransaction(
		    // 1. Set the key to val1
		    [key, val1](auto ctx) {
			    ctx->makeSelfConflicting();
			    ctx->tx().set(key, val1);
			    ctx->commit();
		    },
		    [this, opType, opFunc, key, val1, val2, cont]() {
			    execTransaction(
			        // 2. Perform the given atomic operation to val2, but only if it hasn't been applied yet, otherwise
			        // retries of commit_unknown_result would cause the operation to be applied multiple times, see
			        // https://github.com/apple/foundationdb/issues/1321.
			        [key, opType, val1, val2](auto ctx) {
				        ctx->makeSelfConflicting();
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
		Key keyPrefix(randomKeyName());
		Key key = keyPrefix + fdb::ByteString(10, '\0') + toByteString((uint32_t)keyPrefix.size());
		Value val = randomValue();

		auto versionstamp_f = std::make_shared<fdb::TypedFuture<fdb::future_var::KeyRef>>();
		execTransaction(
		    // 1. Perform SetVersionstampedKey operation.
		    [key, val, versionstamp_f](auto ctx) {
			    ctx->makeSelfConflicting();
			    ctx->tx().atomicOp(key, val, FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);
			    *versionstamp_f = ctx->tx().getVersionstamp();
			    ctx->commit();
		    },
		    [this, keyPrefix, val, versionstamp_f, cont]() {
			    ASSERT(versionstamp_f->ready());
			    auto resultKey = keyPrefix + Key(versionstamp_f->get());
			    auto resultVal = std::make_shared<Value>();
			    execTransaction(
			        // 2. Fetch the resulting versionstamped key and value.
			        [keyPrefix, resultKey, resultVal](auto ctx) {
				        auto fv = ctx->tx().get(resultKey, false);
				        ctx->continueAfter(fv, [ctx, fv, resultVal]() {
					        auto outputVal = fv.get();
					        ASSERT(outputVal.has_value());
					        *resultVal = outputVal.value();

					        ctx->done();
				        });
			        },
			        [this, keyPrefix, val, resultVal, cont]() {
				        // 3. Assert expectation.
				        ASSERT(*resultVal == val);
				        schedule(cont);
			        });
		    });
	}

	void testAtomicVersionstampedValueOp(TTaskFct cont) {
		Key key(randomKeyName());
		Value valPrefix = randomValue();
		Value val = valPrefix + fdb::ByteString(10, '\0') + toByteString((uint32_t)valPrefix.size());
		auto versionstamp_f = std::make_shared<fdb::TypedFuture<fdb::future_var::KeyRef>>();
		execTransaction(
		    // 1. Perform SetVersionstampedValue operation.
		    [key, val, versionstamp_f](auto ctx) {
			    ctx->makeSelfConflicting();
			    ctx->tx().atomicOp(key, val, FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);
			    *versionstamp_f = ctx->tx().getVersionstamp();
			    ctx->commit();
		    },
		    [this, key, valPrefix, versionstamp_f, cont]() {
			    versionstamp_f->blockUntilReady();
			    auto versionstamp = Key(versionstamp_f->get());
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
				        ASSERT(*result == valPrefix + versionstamp);
				        schedule(cont);
			        });
		    });
	}

	void testAtomicCompareAndClearOp(TTaskFct cont) {
		Key key(randomKeyName());
		Value val = randomValue();
		execTransaction(
		    // 1. Set the key to initial value
		    [key, val](auto ctx) {
			    ctx->makeSelfConflicting();
			    ctx->tx().set(key, val);
			    ctx->commit();
		    },
		    [this, key, val, cont]() {
			    execTransaction(
			        // 2. Perform CompareAndClear operation.
			        [key, val](auto ctx) {
				        ctx->makeSelfConflicting();
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
};

WorkloadFactory<AtomicOpsCorrectnessWorkload> AtomicOpsCorrectnessWorkloadFactory("AtomicOpsCorrectness");

} // namespace FdbApiTester
