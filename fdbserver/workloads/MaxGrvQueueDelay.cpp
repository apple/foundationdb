/*
 * MaxGrvQueueDelay.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"

struct MaxGrvQueueDelayWorkload : TestWorkload {
	static constexpr auto NAME = "MaxGrvQueueDelay";

	int requestCount;
	int minRejected;
	int64_t maxQueueDelayMS;
	int64_t permissiveMaxQueueDelayMS;
	double startAfter;
	double testTimeout;

	bool done{ false };
	bool failed{ false };
	PerfIntCounter requests;
	PerfIntCounter successes;
	PerfIntCounter rejected;
	PerfIntCounter unexpectedErrors;

	MaxGrvQueueDelayWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), requests("Requests"), successes("Successes"), rejected("Rejected"),
	    unexpectedErrors("UnexpectedErrors") {
		requestCount = getOption(options, "requestCount"_sr, 50);
		minRejected = getOption(options, "minRejected"_sr, 1);
		maxQueueDelayMS = getOption(options, "maxQueueDelayMS"_sr, int64_t{ 0 });
		permissiveMaxQueueDelayMS = getOption(options, "permissiveMaxQueueDelayMS"_sr, int64_t{ 60000 });
		startAfter = getOption(options, "startAfter"_sr, 5.0);
		testTimeout = getOption(options, "testTimeout"_sr, 30.0);
	}

	static bool runTest() { return g_network->isSimulated() && !isGeneralBuggifyEnabled(); }

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override {
		TraceEvent("MaxGrvQueueDelaySetup")
		    .detail("RequestCount", requestCount)
		    .detail("MinRejected", minRejected)
		    .detail("MaxQueueDelayMS", maxQueueDelayMS)
		    .detail("PermissiveMaxQueueDelayMS", permissiveMaxQueueDelayMS)
		    .detail("StartAfter", startAfter)
		    .detail("TestTimeout", testTimeout);
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0 || !runTest()) {
			return Void();
		}
		return timeout(reportErrors(run(cx), "MaxGrvQueueDelayError"), testTimeout, Void());
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0 || !runTest()) {
			return true;
		}
		if (!done) {
			TraceEvent(SevError, "MaxGrvQueueDelayCheckFailed")
			    .detail("Reason", "WorkloadDidNotComplete")
			    .detail("Rejected", rejected.getValue())
			    .detail("Successes", successes.getValue())
			    .detail("UnexpectedErrors", unexpectedErrors.getValue());
			return false;
		}
		return !failed;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Requests", requests.getValue(), Averaged::False);
		m.emplace_back("Successes", successes.getValue(), Averaged::False);
		m.emplace_back("Rejected", rejected.getValue(), Averaged::False);
		m.emplace_back("Unexpected Errors", unexpectedErrors.getValue(), Averaged::False);
	}

	Future<Version> getReadVersion(Database cx, Optional<int64_t> maxDelayMS) {
		Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::SKIP_GRV_CACHE);
		if (maxDelayMS.present()) {
			int64_t optionValue = maxDelayMS.get();
			tr.setOption(FDBTransactionOptions::MAX_GRV_QUEUE_DELAY,
			             StringRef(reinterpret_cast<uint8_t*>(&optionValue), sizeof(optionValue)));
		}
		Version version = co_await tr.getReadVersion();
		co_return version;
	}

	Future<Void> verifyBaseline(Database cx) {
		ErrorOr<Version> withoutOption = co_await errorOr(getReadVersion(cx, Optional<int64_t>()));
		if (withoutOption.isError()) {
			failed = true;
			++unexpectedErrors;
			TraceEvent(SevError, "MaxGrvQueueDelayBaselineFailed")
			    .error(withoutOption.getError())
			    .detail("HasOption", false);
			co_return;
		}
		++successes;

		ErrorOr<Version> permissive =
		    co_await errorOr(getReadVersion(cx, Optional<int64_t>(permissiveMaxQueueDelayMS)));
		if (permissive.isError()) {
			failed = true;
			++unexpectedErrors;
			TraceEvent(SevError, "MaxGrvQueueDelayBaselineFailed")
			    .error(permissive.getError())
			    .detail("HasOption", true)
			    .detail("MaxQueueDelayMS", permissiveMaxQueueDelayMS);
			co_return;
		}
		++successes;
	}

	Future<Void> run(Database cx) {
		co_await delay(startAfter);
		co_await verifyBaseline(cx);
		if (failed) {
			done = true;
			co_return;
		}

		std::vector<Future<ErrorOr<Version>>> futures;
		futures.reserve(requestCount);
		for (int i = 0; i < requestCount; ++i) {
			++requests;
			futures.push_back(errorOr(getReadVersion(cx, Optional<int64_t>(maxQueueDelayMS))));
		}

		co_await waitForAll(futures);

		for (auto const& f : futures) {
			ASSERT(f.isReady());
			ErrorOr<Version> result = f.get();
			if (!result.isError()) {
				++successes;
			} else if (result.getError().code() == error_code_transaction_grv_queue_rejected) {
				++rejected;
			} else {
				++unexpectedErrors;
				failed = true;
				TraceEvent(SevError, "MaxGrvQueueDelayUnexpectedError")
				    .error(result.getError())
				    .detail("ExpectedError", error_code_transaction_grv_queue_rejected);
			}
		}

		if (rejected.getValue() < minRejected) {
			failed = true;
			TraceEvent(SevError, "MaxGrvQueueDelayTooFewRejections")
			    .detail("Rejected", rejected.getValue())
			    .detail("MinRejected", minRejected)
			    .detail("Successes", successes.getValue())
			    .detail("Requests", requests.getValue());
		} else {
			TraceEvent("MaxGrvQueueDelayComplete")
			    .detail("Rejected", rejected.getValue())
			    .detail("Successes", successes.getValue())
			    .detail("Requests", requests.getValue());
		}

		done = true;
	}
};

WorkloadFactory<MaxGrvQueueDelayWorkload> MaxGrvQueueDelayWorkloadFactory;
