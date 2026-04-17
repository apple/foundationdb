/*
 * StatusWorkload.cpp
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
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbclient/StatusClient.h"
#include "flow/UnitTest.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/ManagementAPI.h"

struct StatusWorkload : TestWorkload {
	static constexpr auto NAME = "Status";

	double testDuration, requestsPerSecond, maxAcceptableStatusLatency;
	bool enableLatencyBands;

	Future<Void> latencyBandActor;

	PerfIntCounter requests, replies, errors, totalSize;
	double worstLatency = 0;
	Optional<StatusObject> parsedSchema;

	StatusWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), requests("Status requests issued"), replies("Status replies received"),
	    errors("Status Errors"), totalSize("Status reply size sum") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		requestsPerSecond = getOption(options, "requestsPerSecond"_sr, 0.5);
		maxAcceptableStatusLatency = getOption(options, "maxStatusLatency"_sr, 0.0);
		enableLatencyBands = getOption(options, "enableLatencyBands"_sr, deterministicRandom()->random01() < 0.5);
		auto statusSchemaStr = getOption(options, "schema"_sr, JSONSchemas::statusSchema);
		if (!statusSchemaStr.empty()) {
			json_spirit::mValue schema = readJSONStrictly(statusSchemaStr.toString());
			parsedSchema = schema.get_obj();

			// This is sort of a hack, but generate code coverage *requirements* for everything in schema
			schemaCoverageRequirements(parsedSchema.get());
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (enableLatencyBands) {
			latencyBandActor = configureLatencyBands(this, cx);
		}

		return Void();
	}
	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();

		return success(timeout(fetcher(cx, this), testDuration));
	}
	Future<bool> check(Database const& cx) override {
		if (errors.getValue() != 0)
			return false;
		if (maxAcceptableStatusLatency > 0 && worstLatency > maxAcceptableStatusLatency) {
			TraceEvent(SevError, "StatusLatencyExceeded")
			    .detail("WorstLatency", worstLatency)
			    .detail("MaxAcceptable", maxAcceptableStatusLatency);
			return false;
		}
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId != 0)
			return;

		m.push_back(requests.getMetric());
		m.push_back(replies.getMetric());
		m.emplace_back(
		    "Average Reply Size", replies.getValue() ? totalSize.getValue() / replies.getValue() : 0, Averaged::False);
		m.push_back(errors.getMetric());
		m.emplace_back("Worst Latency", worstLatency, Averaged::True);
	}

	static void schemaCoverageRequirements(StatusObject const& schema, std::string schema_path = std::string()) {
		try {
			for (auto& skv : schema) {
				std::string spath = schema_path + "." + skv.first;

				schemaCoverage(spath, false);

				if (skv.second.type() == json_spirit::array_type && !skv.second.get_array().empty()) {
					if (skv.second.get_array()[0].type() != json_spirit::str_type)
						schemaCoverageRequirements(skv.second.get_array()[0].get_obj(), spath + "[0]");
				} else if (skv.second.type() == json_spirit::obj_type) {
					if (skv.second.get_obj().contains("$enum")) {
						for (auto& enum_item : skv.second.get_obj().at("$enum").get_array())
							schemaCoverage(spath + ".$enum." + enum_item.get_str(), false);
					} else
						schemaCoverageRequirements(skv.second.get_obj(), spath);
				}
			}
		} catch (std::exception& e) {
			TraceEvent(SevError, "SchemaCoverageRequirementsException").detail("What", e.what());
			throw unknown_error();
		} catch (...) {
			TraceEvent(SevError, "SchemaCoverageRequirementsException").log();
			throw unknown_error();
		}
	}

	static std::string generateBands() {
		int numBands = deterministicRandom()->randomInt(0, 10);
		std::vector<double> bands;

		while (bands.size() < numBands) {
			bands.push_back(deterministicRandom()->random01() * pow(10, deterministicRandom()->randomInt(-5, 1)));
		}

		std::string result = "\"bands\":[";
		for (int i = 0; i < bands.size(); ++i) {
			if (i > 0) {
				result += ",";
			}

			result += format("%f", bands[i]);
		}

		return result + "]";
	}

	Future<Void> configureLatencyBands(StatusWorkload* self, Database cx) {
		while (true) {
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);

					std::string config =
					    "{"
					    "\"get_read_version\":{" +
					    generateBands() +
					    "},"
					    "\"read\":{" +
					    generateBands() +
					    format(", \"max_key_selector_offset\":%d, \"max_read_bytes\":%d},",
					           deterministicRandom()->randomInt(0, 10000),
					           deterministicRandom()->randomInt(0, 1000000)) +
					    ""
					    "\"commit\":{" +
					    generateBands() +
					    format(", \"max_commit_bytes\":%d", deterministicRandom()->randomInt(0, 1000000)) +
					    "}"
					    "}";

					tr.set(latencyBandConfigKey, ValueRef(config));
					co_await tr.commit();
					tr.reset();

					if (deterministicRandom()->random01() < 0.3) {
						co_return;
					}

					co_await delay(deterministicRandom()->random01() * 120);
				} catch (Error& e) {
					err = e;
				}
				if (err.isValid()) {
					co_await tr.onError(err);
				}
			}
		}
	}

	Future<Void> fetcher(Database cx, StatusWorkload* self) {
		double lastTime = now();

		while (true) {
			co_await poisson(&lastTime, 1.0 / self->requestsPerSecond);
			try {
				// Since we count the requests that start, we could potentially never really hear back?
				++self->requests;
				double issued = now();
				StatusObject result = co_await StatusClient::statusFetcher(cx);
				++self->replies;
				BinaryWriter br(AssumeVersion(g_network->protocolVersion()));
				save(br, result);
				self->totalSize += br.getLength();
				double latency = now() - issued;
				self->worstLatency = std::max(self->worstLatency, latency);
				TraceEvent("StatusWorkloadReply").detail("ReplySize", br.getLength()).detail("Latency", latency);
				std::string errorStr;
				if (self->parsedSchema.present() &&
				    !schemaMatch(self->parsedSchema.get(), result, errorStr, SevError, true)) {
					std::cout << errorStr << std::endl;
					TraceEvent(SevError, "StatusWorkloadValidationFailed")
					    .detail("JSON", json_spirit::write_string(json_spirit::mValue(result)));
				}
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent(SevError, "StatusWorkloadError").error(e);
					++self->errors;
				}
				throw;
			}
		}
	}
};

WorkloadFactory<StatusWorkload> StatusWorkloadFactory;

TEST_CASE("/fdbserver/status/schema/basic") {
	json_spirit::mValue schema =
	    readJSONStrictly("{\"apple\":3,\"banana\":\"foo\",\"sub\":{\"thing\":true},\"arr\":[{\"a\":1,\"b\":2}],\"en\":{"
	                     "\"$enum\":[\"foo\",\"bar\"]},\"mapped\":{\"$map\":{\"x\":true}}}");
	auto check = [&schema](bool expect_ok, std::string t) {
		json_spirit::mValue test = readJSONStrictly(t);
		TraceEvent("SchemaMatch")
		    .detail("Schema", json_spirit::write_string(schema))
		    .detail("Value", json_spirit::write_string(test))
		    .detail("Expect", expect_ok);
		std::string errorStr;
		ASSERT(expect_ok ==
		       schemaMatch(schema.get_obj(), test.get_obj(), errorStr, expect_ok ? SevError : SevInfo, true));
	};
	check(true, "{}");
	check(true, "{\"apple\":4}");
	check(false, "{\"apple\":\"wrongtype\"}");
	check(false, "{\"extrathingy\":1}");
	check(true, "{\"banana\":\"b\",\"sub\":{\"thing\":false}}");
	check(false, "{\"banana\":\"b\",\"sub\":{\"thing\":false, \"x\":0}}");
	check(true, "{\"arr\":[{},{\"a\":0}]}");
	check(false, "{\"arr\":[{\"a\":0},{\"c\":0}]}");
	check(true, "{\"en\":\"bar\"}");
	check(false, "{\"en\":\"baz\"}");
	check(true, "{\"mapped\":{\"item1\":{\"x\":false},\"item2\":{}}}");
	check(false, "{\"mapped\":{\"item1\":{\"x\":false},\"item2\":{\"y\":1}}}");

	return Void();
}
