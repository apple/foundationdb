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
		// This test sets a key to an initial value, sets up a watch and waits for it to be triggered.
		Key key(randomKeyName());
		Value initialVal = randomValue();
		execTransaction(
		    // 1. Set the key to initialVal.
		    [key, initialVal](auto ctx) {
			    ctx->tx().set(key, initialVal);
			    ctx->commit();
		    },
		    [this, key, initialVal, cont]() { watch(key, initialVal, cont); });
	}

	void watch(Key key, Value initialVal, TTaskFct cont) {
		// Assuming key starts with a value initialVal, set up a watch for that key, change its value to a different
		// value newVal, and ensure that the watch was triggered. Upon retryable watch failures retries the whole
		// process again.
		Value newVal = randomValue();
		// Ensure that newVal is different from initialVal, otherwise watch may not trigger.
		while (initialVal == newVal) {
			newVal = randomValue();
		}

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
			        [this, key, newVal, watch_f, cont]() {
				        auto retry = std::make_shared<bool>(false);
				        execTransaction(
				            [this, key, watch_f, retry](auto ctx) {
					            // 4. Wait for the watch future to become ready.
					            // Note that we do this in the context of a dummy transaction. Ideally we would do this
					            // in the context of the transaction which sets up the watch in the first place, but
					            ctx->continueAfter(
					                *watch_f,
					                [this, ctx, watch_f, retry] {
						                // If the watch future finished with a failure, see if it can be retried.
						                if (watch_f->error()) {
							                auto onErrorFuture = ctx->tx().onError(watch_f->error());
							                fdb::Error blockError = onErrorFuture.blockUntilReady();
							                if (blockError) {
								                error(fmt::format("Block error: {}", blockError.what()));
							                } else if (onErrorFuture.error()) {
								                error(fmt::format("Watch error: {}", onErrorFuture.error().what()));
							                } else {
								                *retry = true;
							                }
						                }
						                ctx->done();
					                },
					                false);
				            },
				            [this, retry, key, newVal, cont]() {
					            if (*retry) {
						            watch(key, newVal, cont);
					            } else {
						            schedule(cont);
					            }
				            });
			        });
		    });
	}
};

WorkloadFactory<WatchAndWaitWorkload> WatchAndWaitWorkloadFactory("WatchAndWait");

} // namespace FdbApiTester