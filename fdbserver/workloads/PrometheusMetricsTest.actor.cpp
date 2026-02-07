/*
 * PrometheusMetricsTest.actor.cpp
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

#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/PrometheusMetrics.h"
#include "fdbrpc/HTTP.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

Reference<HTTP::IRequestHandler> makePrometheusMetricsHandler();

struct PrometheusMetricsTestWorkload : TestWorkload {
	static constexpr auto NAME = "PrometheusMetricsTest";
	double testDuration;
	Reference<IConnection> conn;

	std::string hostname = "prometheusmetrics";
	std::string service = "80";

	PerfIntCounter requestCount;

	PrometheusMetricsTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), requestCount("RequestCount") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
	}

	Future<Void> setup(Database const& cx) override { return _setup(this); }

	ACTOR Future<Void> _setup(PrometheusMetricsTestWorkload* self) {
		ASSERT(g_network->isSimulated());

		if (self->clientId == 0) {
			TraceEvent("PrometheusMetricsTestRegistering");
			wait(g_simulator->registerSimHTTPServer(
			    self->hostname, self->service, makePrometheusMetricsHandler()));
			TraceEvent("PrometheusMetricsTestRegistered");
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(this); }

	ACTOR Future<Void> _start(PrometheusMetricsTestWorkload* self) {
		state double startTime = now();

		static SimpleCounter<int64_t>* testCounter =
		    SimpleCounter<int64_t>::makeCounter("/test/prometheus/requests");
		testCounter->increment(1);

		wait(delay(0.5));

		while (now() - startTime < self->testDuration) {
			try {
				wait(self->makeMetricsRequest(self));
				++self->requestCount;
			} catch (Error& e) {
				if (e.code() == error_code_operation_cancelled) {
					throw e;
				}
				TraceEvent("PrometheusMetricsTestRequestFailed").error(e);
				wait(delay(0.5));
			}
			wait(delay(1.0));
		}

		return Void();
	}

	ACTOR Future<Void> makeMetricsRequest(PrometheusMetricsTestWorkload* self) {
		state Reference<IConnection> conn;

		wait(store(conn,
		           timeoutError(INetworkConnections::net()->connect(self->hostname, self->service, false),
		                        FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT)));
		ASSERT(conn.isValid());
		wait(conn->connectHandshake());

		state UnsentPacketQueue content;
		state Reference<HTTP::OutgoingRequest> req = makeReference<HTTP::OutgoingRequest>();
		req->data.content = &content;
		req->data.contentLen = 0;
		req->verb = HTTP::HTTP_VERB_GET;
		req->resource = "/metrics";

		state Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
		state int64_t bytes_sent = 0;

		Reference<HTTP::IncomingResponse> response =
		    wait(timeoutError(HTTP::doRequest(conn, req, sendReceiveRate, &bytes_sent, sendReceiveRate), 5.0));

		ASSERT_EQ(response->code, 200);
		ASSERT(response->data.headers.contains("Content-Type"));
		ASSERT(response->data.headers["Content-Type"].find("text/plain") != std::string::npos);

		std::string body = response->data.content;
		TraceEvent("PrometheusMetricsTestResponse")
		    .detail("ContentLength", body.size())
		    .detail("SampleContent", body.substr(0, std::min(body.size(), size_t(200))));

		ASSERT(body.size() > 0);
		ASSERT(body.find(" ") != std::string::npos);

		conn->close();

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override { m.push_back(requestCount.getMetric()); }
};

WorkloadFactory<PrometheusMetricsTestWorkload> PrometheusMetricsTestWorkloadFactory;
