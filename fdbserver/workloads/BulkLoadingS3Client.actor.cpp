/*
 * BulkLoadingS3Client.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
// #include "fdbclient/S3Client.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct BulkLoadingS3Client : TestWorkload {
	static constexpr auto NAME = "BulkLoadingS3ClientWorkload";
	const bool enabled;
	bool pass;

	BulkLoadingS3Client(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> _start(BulkLoadingS3Client* self, Database cx) {
		if (self->clientId != 0) {
			// Our simulation test can trigger multiple same workloads at the same time
			// Only run one time workload in the simulation
			return Void();
		}

		/* Add S3 function call here */
		/* Need to have an alive seaweed running with simulation */
		TraceEvent("BulkLoadingS3ClientWorkloadStart");
		wait(delay(0.1)); // code place holder for passing ACTOR compiling

		// Run this workload with ../build_output/bin/fdbserver -r simulation -f
		// ../src/foundationdb/tests/fast/BulkLoadingS3Client.toml

		return Void();
	}
};

WorkloadFactory<BulkLoadingS3Client> BulkLoadingS3ClientFactory;
