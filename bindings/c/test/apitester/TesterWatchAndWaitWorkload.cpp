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
		// The test will set key to initialVal, then set up a watch on that key, change the key to newVal and verify
		// that watch was called.
		Key key(randomKeyName());
		Value initialVal = randomValue();
		Value newVal = randomValue();
		// Ensure that newVal is different from initialVal, otherwise watch may not trigger.
		while (initialVal == newVal) {
			newVal = randomValue();
		}
		execTransaction(
		    // 1. Set the key to initialVal.
		    [key, initialVal](auto ctx) {
			    ctx->tx().set(key, initialVal);
			    ctx->commit();
		    },
		    [this, key, newVal, cont]() {
			    auto watch_f = std::make_shared<fdb::TypedFuture<fdb::future_var::None>>();

			    execTransaction(
			        // 2. Create a watch for key.
			        [key, watch_f](auto ctx) {
				        *watch_f = ctx->tx().watch(key);
				        ctx->commit();
			        },
			        [this, key, newVal, watch_f, cont]() {
				        execTransaction(
				            // 3. Set the key to a newVal which is guaranteed to be different from initialVal, i.e.,
				            // must trigger the watch.
				            [key, newVal](auto ctx) {
					            ctx->tx().set(key, newVal);
					            ctx->commit();
				            },
				            [this, watch_f, cont]() {
					            execTransaction(
					                [watch_f](auto ctx) {
						                // 4. Wait for the watch future to become ready.
						                ctx->continueAfter(*watch_f, [ctx] { ctx->done(); });
					                },
					                [this, cont]() { schedule(cont); });
				            });
			        });
		    });
	}
};

WorkloadFactory<WatchAndWaitWorkload> WatchAndWaitWorkloadFactory("WatchAndWait");

} // namespace FdbApiTester