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

#include <array>
#include <string_view>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "fdbclient/StatusClient.h"
#include "flow/UnitTest.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/StatusSchema.h"

struct StatusWorkload : TestWorkload {
	static constexpr auto NAME = "Status";

	double testDuration, requestsPerSecond, maxAcceptableStatusLatency;
	bool enableLatencyBands;

	Future<Void> latencyBandActor;

	PerfIntCounter requests, replies, errors, totalSize;
	double worstLatency = 0;
	Optional<StatusObject> parsedSchema;

	explicit StatusWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), requests("Status requests issued"), replies("Status replies received"),
	    errors("Status Errors"), totalSize("Status reply size sum") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		requestsPerSecond = getOption(options, "requestsPerSecond"_sr, 0.5);
		maxAcceptableStatusLatency = getOption(options, "maxAcceptableStatusLatency"_sr, 0.0);
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

	static bool shouldTrackSchemaCoverage(std::string_view path,
	                                      bool simulated,
	                                      json_spirit::Value_type schemaType = json_spirit::obj_type) {
		if (path.ends_with(".$map") && schemaType != json_spirit::obj_type) {
			return false;
		}

		if (!simulated) {
			return true;
		}

		if (path.starts_with(".cluster.machines.$map.")) {
			return false;
		}

		constexpr std::array productionOnlyPaths{
			std::string_view(".cluster.processes.$map.cpu"),
			std::string_view(".cluster.processes.$map.disk"),
			std::string_view(".cluster.processes.$map.network"),
			std::string_view(".cluster.processes.$map.locality"),
			std::string_view(".cluster.processes.$map.command_line"),
			std::string_view(".cluster.processes.$map.fault_domain"),
			std::string_view(".cluster.processes.$map.machine_id"),
			std::string_view(".cluster.processes.$map.run_loop_busy"),
			std::string_view(".cluster.processes.$map.under_maintenance"),
			std::string_view(".cluster.processes.$map.uptime_seconds"),
			std::string_view(".cluster.processes.$map.version"),
			std::string_view(".cluster.processes.$map.memory.available_bytes"),
			std::string_view(".cluster.processes.$map.memory.limit_bytes"),
			std::string_view(".cluster.processes.$map.memory.rss_bytes"),
			std::string_view(".cluster.processes.$map.memory.unused_allocated_memory"),
			std::string_view(".cluster.processes.$map.memory.used_bytes"),
		};
		for (auto productionOnlyPath : productionOnlyPaths) {
			if (path == productionOnlyPath ||
			    (path.starts_with(productionOnlyPath) && path[productionOnlyPath.size()] == '.')) {
				return false;
			}
		}

		return true;
	}

	static void schemaCoverageRequirements(StatusObject const& schema, std::string schema_path = std::string()) {
		try {
			for (auto& skv : schema) {
				std::string spath = schema_path + "." + skv.first;
				if (!shouldTrackSchemaCoverage(spath, g_network->isSimulated(), skv.second.type())) {
					continue;
				}

				schemaCoverage(spath, false);

				if (skv.second.type() == json_spirit::array_type && !skv.second.get_array().empty()) {
					if (skv.second.get_array()[0].type() != json_spirit::str_type)
						schemaCoverageRequirements(skv.second.get_array()[0].get_obj(), spath + "[0]");
				} else if (skv.second.type() == json_spirit::obj_type) {
					if (skv.second.get_obj().contains("$enum")) {
						for (auto& enum_item : skv.second.get_obj().at("$enum").get_array()) {
							schemaCoverage(spath + ".$enum." + enum_item.get_str(), false);
						}
					} else {
						schemaCoverageRequirements(skv.second.get_obj(), spath);
					}
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

TEST_CASE("/fdbserver/status/schema/coverage") {
	for (bool simulated : { false, true }) {
		ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.roles[0].role.$enum.storage",
		                                                 simulated));
		ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.class_type.$enum.blob_worker",
		                                                 simulated));
		ASSERT(
		    StatusWorkload::shouldTrackSchemaCoverage(".cluster.configuration.storage_engine.$enum.ssd-1", simulated));
		ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.configuration.storage_engine.$enum.memory-radixtree",
		                                                 simulated));
		ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.other_engine.$enum.ssd", simulated));
		ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(
		    ".cluster.processes.$map.roles[0].commit_latency_bands.$map", simulated, json_spirit::int_type));
		ASSERT(StatusWorkload::shouldTrackSchemaCoverage(
		    ".cluster.processes.$map.roles[0].commit_latency_bands.$map", simulated, json_spirit::obj_type));
		ASSERT(StatusWorkload::shouldTrackSchemaCoverage(
		    ".cluster.processes.$map.roles[0].commit_latency_bands", simulated, json_spirit::int_type));
	}

	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.machines", true));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.machines.$map", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.machines.$map.memory.total_bytes", true));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.machines.$map.memory.total_bytes", false));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.cpu", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.disk.reads.counter", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.network.megabits_sent.hz", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.locality.$map", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.memory.used_bytes", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.memory.limit_bytes", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.command_line", true));
	ASSERT(!StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.under_maintenance", true));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.cpu", false));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.memory.used_bytes", false));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.command_line", false));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.memory", true));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.processes.$map.cpu_limit", true));
	ASSERT(StatusWorkload::shouldTrackSchemaCoverage(".cluster.clients.supported_versions", true));

	return Void();
}

TEST_CASE("/fdbserver/status/schema/canonical_outputs") {
	json_spirit::mValue schema = readJSONStrictly(JSONSchemas::statusSchema.toString());
	auto check = [&schema](bool expectOk, std::string const& response) {
		json_spirit::mValue result = readJSONStrictly(response);
		std::string errorStr;
		ASSERT(expectOk == schemaMatch(schema, result, errorStr, expectOk ? SevError : SevInfo));
	};
	auto checkConfigurationEngine = [&check](bool expectOk, std::string const& field, std::string const& engine) {
		check(expectOk, R"({"cluster":{"configuration":{")" + field + R"(":")" + engine + R"("}}})");
	};

	check(false, R"({"cluster":{"processes":{"process1":{"roles":[{"role":"blob_manager"}]}}}})");
	check(false, R"({"cluster":{"processes":{"process1":{"roles":[{"role":"blob_worker"}]}}}})");
	check(true, R"({"cluster":{"processes":{"process1":{"roles":[{"role":"storage"}]}}}})");
	check(true, R"({"cluster":{"processes":{"process1":{"class_type":"blob_worker"}}}})");

	for (auto const& field :
	     { "log_engine", "storage_engine", "tss_storage_engine", "perpetual_storage_wiggle_engine" }) {
		checkConfigurationEngine(true, field, "ssd-1");
		checkConfigurationEngine(false, field, "ssd");
		checkConfigurationEngine(false, field, "memory-1");
		checkConfigurationEngine(false, field, "memory-2");
	}
	checkConfigurationEngine(true, "storage_engine", "memory-radixtree");
	checkConfigurationEngine(true, "tss_storage_engine", "memory-radixtree");
	checkConfigurationEngine(true, "perpetual_storage_wiggle_engine", "memory-radixtree");
	checkConfigurationEngine(true, "perpetual_storage_wiggle_engine", "none");
	checkConfigurationEngine(false, "perpetual_storage_wiggle_engine", "memory-radixtree-beta");

	check(false, R"({"cluster":{"processes":{"process1":{"roles":[{"storage_metadata":{"storage_engine":"ssd"}}]}}}})");
	check(true,
	      R"({"cluster":{"processes":{"process1":{"roles":[{"storage_metadata":{"storage_engine":"ssd-1"}}]}}}})");
	check(
	    true,
	    R"({"cluster":{"processes":{"process1":{"roles":[{"storage_metadata":{"storage_engine":"memory-radixtree"}}]}}}})");

	json_spirit::mValue configurationSchema = readJSONStrictly(JSONSchemas::clusterConfigurationSchema.toString());
	json_spirit::mValue configuration = readJSONStrictly(R"({"storage_engine":"ssd"})");
	std::string errorStr;
	ASSERT(schemaMatch(configurationSchema, configuration, errorStr));

	return Void();
}

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
