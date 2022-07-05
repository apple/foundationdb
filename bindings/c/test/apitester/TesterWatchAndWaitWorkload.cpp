/*
 * TesterWatchAndWaitWorkload.cpp
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
#include "test/fdb_api.hpp"

namespace FdbApiTester {

using fdb::Key;
using fdb::Value;

class WatchAndWaitWorkload : public ApiWorkload {
public:
	WatchAndWaitWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	void randomOperation(TTaskFct cont) override {
		// This test sets a key to an initial value, sets up a watch for that key, change the key's value to a different
		// value, and waits for the watch to be triggered.
		Key key(randomKeyName());
		Value initialVal = randomValue();
		execTransaction(
		    [key, initialVal](auto ctx) {
			    // 1. Set the key to initialVal.
			    ctx->tx().set(key, initialVal);
			    ctx->commit();
		    },
		    [this, key, initialVal, cont]() {
			    execTransaction(
			        [this, key, initialVal](auto ctx) {
				        // 2. Create a watch for key.

				        auto f = ctx->tx().watch(key);
				        ctx->continueAfter(f, [ctx] {
					        // Complete the transaction context upon watch callback.
					        ctx->done();
				        });
				        // Do not automatically complete the context upon commit. Context will be completed by watch
				        // callback.
				        ctx->commit(/*complete=*/false);

				        auto newVal = randomValue();
				        // Ensure that newVal is different from initialVal, otherwise watch may not trigger.
				        while (initialVal == newVal) {
					        newVal = randomValue();
				        }
				        schedule([this, key, newVal] {
					        execTransaction(
					            // 3. Set the key to a newVal which is guaranteed to be different from initialVal, i.e.,
					            // must trigger the watch.
					            [key, newVal](auto ctx) {
						            ctx->tx().set(key, newVal);
						            ctx->commit();
					            },
					            []() {});
				        });
			        },
			        [this, cont]() { schedule(cont); });
		    });
	}
};

WorkloadFactory<WatchAndWaitWorkload> WatchAndWaitWorkloadFactory("WatchAndWait");

} // namespace FdbApiTester
