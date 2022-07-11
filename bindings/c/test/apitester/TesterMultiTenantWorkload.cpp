/*
 * TesterMultiTenantWorkload.cpp
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
#include "test/fdb_api.hpp"
#include <memory>
#include <fmt/format.h>

namespace FdbApiTester {

class MultiTenantWorkload : public ApiWorkload {
public:
	MultiTenantWorkload(const WorkloadConfig& config) : ApiWorkload(config) {}

private:
	enum OpType { OP_INSERT, OP_CLEAR, OP_CLEAR_RANGE, OP_LAST = OP_CLEAR_RANGE };

	void randomOperation(TTaskFct cont) {
		OpType txType = (store.size() == 0) ? OP_INSERT : (OpType)Random::get().randomInt(0, OP_LAST);
		switch (txType) {
		case OP_INSERT:
			randomInsertOp(cont);
			break;
		case OP_CLEAR:
			randomClearOp(cont);
			break;
		case OP_CLEAR_RANGE:
			randomClearRangeOp(cont);
			break;
		}
	}
};

WorkloadFactory<MultiTenantWorkload> MultiTenantWorkloadFactory("MultiTenant");

} // namespace FdbApiTester
