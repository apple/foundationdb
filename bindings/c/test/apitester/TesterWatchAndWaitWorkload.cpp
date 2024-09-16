/*
 * TesterWatchAndWaitWorkload.cpp
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
#include "test/fdb_api.hpp"

namespace FdbApiTester {

using fdb::Key;
using fdb::Value;

class WatchAndWaitWorkload : public ApiWorkload {
public:
	WatchAndWaitWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}
	int getMaxSelfBlockingFutures() override {
		// One watch future running concurrently waits for a commit of a transaction which sets the value.
		return 1;
	}

private:
	void randomOperation(TTaskFct cont) override {
		// This test sets a key to an initial value, sets up a watch for that key, change the key's value to a different
		// value, and waits for the watch to be triggered.
		Key key(randomKeyName());
		Value initialVal = randomValue();
		auto newVal = randomValue();
		// Ensure that newVal is different from initialVal, otherwise watch may not trigger.
		while (initialVal == newVal) {
			newVal = randomValue();
		}

		execTransaction(
		    [key, initialVal](auto ctx) {
			    // Set the key to initialVal.
			    ctx->tx().set(key, initialVal);
			    ctx->commit();
		    },
		    [this, key, newVal, cont]() {
			    execTransaction(
			        [key, newVal](auto ctx) {
				        // Check the value of the key.
				        auto f = ctx->tx().get(key, false);
				        ctx->continueAfter(f, [key, f, newVal, ctx] {
					        ASSERT(f.get().has_value());
					        if (f.get().value() == newVal) {
						        // If the key is already at newVal, finish successfully.
						        ctx->done();
					        } else {
						        // Otherwise, create a watch for the key.
						        auto watchF = ctx->tx().watch(key);
						        auto commitF = ctx->tx().commit();

						        ctx->continueAfterAll({ commitF, watchF }, [ctx] {
							        // Wait for the watch to report a change (to newVal).
							        ctx->done();
						        });
					        }
				        });
			        },
			        [this, cont]() { schedule(cont); });
			    schedule([this, key, newVal] {
				    execTransaction(
				        // Set the key to a newVal which is guaranteed to be different from initialVal, i.e.,
				        // must trigger the watch.
				        [key, newVal](auto ctx) {
					        ctx->tx().set(key, newVal);
					        ctx->commit();
				        },
				        []() {});
			    });
		    });
	}
};

WorkloadFactory<WatchAndWaitWorkload> WatchAndWaitWorkloadFactory("WatchAndWait");

} // namespace FdbApiTester
