/*
 * MockDDTest.h
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
#ifndef FOUNDATIONDB_MOCKDDTEST_H
#define FOUNDATIONDB_MOCKDDTEST_H

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/DDSharedContext.h"
#include "fdbserver/DDTxnProcessor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbclient/StorageServerInterface.h"

// other Mock DD workload can derive from this class to use the common settings
struct MockDDTestWorkload : public TestWorkload {
	bool enabled;
	bool simpleConfig;
	double testDuration;
	double meanDelay = 0.05;
	double maxKeyspace = 0.1; // range space
	int maxByteSize = 1024, minByteSize = 32; // single point value size. The Key size is fixed to 16 bytes

	std::shared_ptr<MockGlobalState> mgs;
	Reference<DDMockTxnProcessor> mock;

	KeyRange getRandomRange(double offset) const;
	Future<Void> setup(Database const& cx) override;

protected:
	MockDDTestWorkload(WorkloadContext const& wcx);
};

#endif // FOUNDATIONDB_MOCKDDTEST_H
