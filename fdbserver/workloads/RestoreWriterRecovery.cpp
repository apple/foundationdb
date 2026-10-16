/*
 * RestoreWriterRecovery.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/tester/workloads.h"

namespace fileBackup {
Future<Void> testRestoreWriterRecovery(Database cx);
}

struct RestoreWriterRecoveryWorkload : TestWorkload {
	static constexpr auto NAME = "RestoreWriterRecovery";
	explicit RestoreWriterRecoveryWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		return clientId == 0 ? fileBackup::testRestoreWriterRecovery(cx) : Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RestoreWriterRecoveryWorkload> RestoreWriterRecoveryWorkloadFactory;
