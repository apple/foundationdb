/*
 * LocalRatekeeper.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbserver/core/Knobs.h"

namespace {

Future<StorageServerInterface> getRandomStorage(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			RangeResult range = co_await tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			if (!range.empty()) {
				auto idx = deterministicRandom()->randomInt(0, range.size());
				co_return decodeServerListValue(range[idx].value);
			} else {
				co_await delay(1.0);
			}
		} catch (Error& e) {
			err = e;
		}
		if (err.isValid()) {
			co_await tr.onError(err);
		}
	}
}

struct LocalRatekeeperWorkload : TestWorkload {
	static constexpr auto NAME = "LocalRatekeeper";

	double startAfter = 0.0;
	double blockWritesFor;
	bool testFailed = false;

	LocalRatekeeperWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		startAfter = getOption(options, "startAfter"_sr, startAfter);
		blockWritesFor = getOption(
		    options, "blockWritesFor"_sr, double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX) / double(1e6));
	}

	Future<Void> testStorage(Database cx, StorageServerInterface ssi) {
		Transaction tr(cx);
		std::vector<Future<GetValueReply>> requests;
		requests.reserve(100);
		while (true) {
			StorageQueuingMetricsReply metrics =
			    co_await brokenPromiseToNever(ssi.getQueuingMetrics.getReply(StorageQueuingMetricsRequest{}));
			auto durabilityLag = metrics.version - metrics.durableVersion;
			double expectedRateLimit = 1.0;
			if (durabilityLag >= SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX) {
				expectedRateLimit = 0.0;
			} else if (durabilityLag > SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX) {
				expectedRateLimit = 1.0 - double(durabilityLag - SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX) /
				                              double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX -
				                                     SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX);
			}
			if (expectedRateLimit < metrics.localRateLimit - 0.01 ||
			    expectedRateLimit > metrics.localRateLimit + 0.01) {
				testFailed = true;
				TraceEvent(SevError, "StorageRateLimitTooFarOff")
				    .detail("Storage", ssi.id())
				    .detail("Expected", expectedRateLimit)
				    .detail("Actual", metrics.localRateLimit);
			}
			tr.reset();
			Version readVersion = invalidVersion;
			while (true) {
				Error err;
				try {
					Version v = co_await tr.getReadVersion();
					readVersion = v;
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
			requests.clear();
			// we send 100 requests to this storage node and count how many of those get rejected
			for (int i = 0; i < 100; ++i) {
				GetValueRequest req;
				req.version = readVersion;
				// we don't care about the value
				req.key = "/lkfs"_sr;
				requests.emplace_back(brokenPromiseToNever(ssi.getValue.getReply(req)));
			}
			co_await waitForAllReady(requests);
			int failedRequests = 0;
			int errors = 0;
			for (const auto& resp : requests) {
				if (resp.isError()) {
					testFailed = true;
					++errors;
					TraceEvent(SevError, "LoadBalancedResponseReturnedError").error(resp.getError());
				} else if (resp.get().error.present() && resp.get().error.get().code() == error_code_future_version) {
					++failedRequests;
				}
				if (errors > 9) {
					break;
				}
			}
			TraceEvent("RejectedVersions").detail("NumRejected", failedRequests);
			if (testFailed) {
				co_return;
			}
			co_await delay(5.0);
		}
	}

	Future<Void> _start(Database cx) {
		co_await delay(startAfter);
		StorageServerInterface ssi = co_await getRandomStorage(cx);
		g_simulator->disableFor(format("%s/updateStorage", ssi.id().toString().c_str()), now() + blockWritesFor);
		Future<Void> done = delay(blockWritesFor);
		// not much will happen until the storage goes over the soft limit
		co_await delay(double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX / 1e6));
		co_await (testStorage(cx, ssi) || done);
	}

	Future<Void> start(Database const& cx) override {
		// we run this only on one client
		if (clientId != 0 || !g_network->isSimulated()) {
			return Void();
		}
		return _start(cx);
	}
	Future<bool> check(Database const& cx) override { return !testFailed; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

} // namespace

WorkloadFactory<LocalRatekeeperWorkload> LocalRatekeeperWorkloadFactory;
