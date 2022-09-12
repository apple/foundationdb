/*
 * TesterExampleWorkload.cpp
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

#include "TesterWorkload.h"
#include "TesterUtil.h"

namespace FdbApiTester {

class SetAndGetWorkload : public WorkloadBase {
public:
	fdb::Key keyPrefix;
	Random random;

	SetAndGetWorkload(const WorkloadConfig& config) : WorkloadBase(config) {
		keyPrefix = fdb::toBytesRef(fmt::format("{}/", workloadId));
	}

	void start() override { setAndGet(NO_OP_TASK); }

	void setAndGet(TTaskFct cont) {
		fdb::Key key = keyPrefix + random.randomByteStringLowerCase(10, 100);
		fdb::Value value = random.randomByteStringLowerCase(10, 1000);
		execTransaction(
		    [key, value](auto ctx) {
			    ctx->tx().set(key, value);
			    ctx->commit();
		    },
		    [this, key, value, cont]() {
			    execTransaction(
			        [this, key, value](auto ctx) {
				        auto future = ctx->tx().get(key, false);
				        ctx->continueAfter(future, [this, ctx, future, value]() {
					        std::optional<fdb::Value> res = copyValueRef(future.get());
					        if (res != value) {
						        error(fmt::format(
						            "expected: {} actual: {}", fdb::toCharsRef(value), fdb::toCharsRef(res.value())));
					        }
					        ctx->done();
				        });
			        },
			        cont);
		    });
	}
};

WorkloadFactory<SetAndGetWorkload> SetAndGetWorkloadFactory("SetAndGet");

} // namespace FdbApiTester
