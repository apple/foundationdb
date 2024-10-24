/*
 * Cache.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct CacheWorkload : TestWorkload {
	static constexpr auto NAME = "Cache";
	Key keyPrefix;

	CacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			// Call management API to cache keys under the given prefix
			return ManagementAPI::addCachedRange(cx.getReference(), prefixRange(keyPrefix));
		}
		return Void();
	}
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<CacheWorkload> CacheWorkloadFactory;
