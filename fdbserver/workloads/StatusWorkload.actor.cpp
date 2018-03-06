/*
 * StatusWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "workloads.h"
#include "fdbclient/StatusClient.h"
#include "flow/UnitTest.h"

extern bool noUnseed;

struct StatusWorkload : TestWorkload {
	double testDuration, requestsPerSecond;

	PerfIntCounter requests, replies, errors, totalSize;
	Optional<StatusObject> statusSchema;

	StatusWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx),
		requests("Status requests issued"), replies("Status replies received"), errors("Status Errors"), totalSize("Status reply size sum")
	{
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		requestsPerSecond = getOption(options, LiteralStringRef("requestsPerSecond"), 0.5);
		auto statusSchemaStr = getOption(options, LiteralStringRef("schema"), StringRef());
		if (statusSchemaStr.size()) {
			json_spirit::mValue schema;
			json_spirit::read_string( statusSchemaStr.toString(), schema );
			statusSchema = schema.get_obj();

			// This is sort of a hack, but generate code coverage *requirements* for everything in schema
			schemaCoverageRequirements(statusSchema.get());
		}

		noUnseed = true;
	}

	virtual std::string description() { return "StatusWorkload"; }
	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}
	virtual Future<Void> start(Database const& cx) {
		if (clientId != 0)
			return Void();
		Reference<Cluster> cluster = cx->cluster;
		if (!cluster) {
			TraceEvent(SevError, "StatusWorkloadStartError").detail("Reason", "NULL cluster");
			return Void();
		}
		return success(timeout(fetcher(cluster->getConnectionFile(), this), testDuration));
	}
	virtual Future<bool> check(Database const& cx) {
		return errors.getValue() == 0;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
		if (clientId != 0)
			return;

		m.push_back(requests.getMetric());
		m.push_back(replies.getMetric());
		m.push_back(PerfMetric("Average Reply Size", replies.getValue() ? totalSize.getValue() / replies.getValue() : 0, false));
		m.push_back(errors.getMetric());
	}

	template <bool Covered=true>
	static void schemaCoverage( std::string const& spath ) {
		static std::set<std::string> coveredSchemaPaths;
		if (coveredSchemaPaths.insert(spath).second) {
			TraceEvent ev(SevInfo, "CodeCoverage");
			ev.detail("File", "documentation/StatusSchema.json/" + spath).detail("Line", 0);
			if (!Covered)
				ev.detail("Covered", 0);
		}
	}

	static void schemaCoverageRequirements( StatusObject const& schema, std::string schema_path = std::string() ) {
		try {
			for(auto& skv : schema) {
				std::string spath = schema_path + "." + skv.first;

				schemaCoverage<false>(spath);

				if (skv.second.type() == json_spirit::array_type && skv.second.get_array().size()) {
					schemaCoverageRequirements( skv.second.get_array()[0].get_obj(), spath + "[0]" );
				} else if (skv.second.type() == json_spirit::obj_type) {
					if (skv.second.get_obj().count("$enum")) {
						for(auto& enum_item : skv.second.get_obj().at("$enum").get_array())
							schemaCoverage<false>(spath + ".$enum." + enum_item.get_str());
					} else
						schemaCoverageRequirements( skv.second.get_obj(), spath );
				}
			}
		} catch (std::exception& e) {
			TraceEvent(SevError,"schemaCoverageRequirementsException").detail("What", e.what());
			throw unknown_error();
		} catch (...) {
			TraceEvent(SevError,"schemaCoverageRequirementsException");
			throw unknown_error();
		}
	}

	static json_spirit::Value_type normJSONType(json_spirit::Value_type type) {
		if (type == json_spirit::int_type)
			return json_spirit::real_type;
		return type;
	}

	static bool schemaMatch( StatusObject const schema, StatusObject const result, Severity sev=SevError, std::string path = std::string(), std::string schema_path = std::string() ) {
		// Returns true if everything in `result` is permitted by `schema`

		// Really this should recurse on "values" rather than "objects"?

		bool ok = true;

		try {
			for(auto& rkv : result) {
				auto& key = rkv.first;
				auto& rv = rkv.second;
				std::string kpath = path + "." + key;
				std::string spath = schema_path + "." + key;

				schemaCoverage(spath);

				if (!schema.count(key)) {
					TraceEvent(sev, "SchemaMismatch").detail("path", kpath).detail("schema_path", spath);
					ok = false;
					continue;
				}
				auto& sv = schema.at(key);

				if (sv.type() == json_spirit::obj_type && sv.get_obj().count("$enum")) {
					auto& enum_values = sv.get_obj().at("$enum").get_array();

					bool any_match = false;
					for(auto& enum_item : enum_values)
						if (enum_item == rv) {
							any_match = true;
							schemaCoverage(spath + ".$enum." + enum_item.get_str());
							break;
						}
					if (!any_match) {
						TraceEvent(sev, "SchemaMismatch").detail("path", kpath).detail("SchemaEnumItems", enum_values.size()).detail("Value", json_spirit::write_string(rv));
						schemaCoverage(spath + ".$enum." + json_spirit::write_string(rv));
						ok = false;
					}
				} else if (sv.type() == json_spirit::obj_type && sv.get_obj().count("$map")) {
					if (rv.type() != json_spirit::obj_type) {
						TraceEvent(sev, "SchemaMismatch").detail("path", kpath).detail("SchemaType", sv.type()).detail("ValueType", rv.type());
						ok = false;
						continue;
					}
					
					if(sv.get_obj().at("$map").type() != json_spirit::obj_type) {
						continue;
					}
					
					auto& schema_obj = sv.get_obj().at("$map").get_obj();
					auto& value_obj = rv.get_obj();

					schemaCoverage(spath + ".$map");

					for(auto& value_pair : value_obj) {
						auto vpath = kpath + "[" + value_pair.first + "]";
						auto upath = spath + ".$map";
						if (value_pair.second.type() != json_spirit::obj_type) {
							TraceEvent(sev, "SchemaMismatch").detail("path", vpath).detail("ValueType", value_pair.second.type());
							ok = false;
							continue;
						}
						if (!schemaMatch(schema_obj, value_pair.second.get_obj(), sev, vpath, upath))
							ok = false;
					}
				} else {
					// The schema entry isn't an operator, so it asserts a type and (depending on the type) recursive schema definition
					if (normJSONType(sv.type()) != normJSONType(rv.type())) {
						TraceEvent(sev, "SchemaMismatch").detail("path", kpath).detail("SchemaType", sv.type()).detail("ValueType", rv.type());
						ok = false;
						continue;
					}
					if (rv.type() == json_spirit::array_type) {
						auto& value_array = rv.get_array();
						auto& schema_array = sv.get_array();
						if (!schema_array.size()) {
							// An empty schema array means that the value array is required to be empty
							if (value_array.size()) {
								TraceEvent(sev, "SchemaMismatch").detail("path", kpath).detail("SchemaSize", schema_array.size()).detail("ValueSize", value_array.size());
								ok = false;
								continue;
							}
						} else if (schema_array.size() == 1 && schema_array[0].type() == json_spirit::obj_type) {
							// A one item schema array means that all items in the value must match the first item in the schema
							auto& schema_obj = schema_array[0].get_obj();
							int index = 0;
							for(auto &value_item : value_array) {
								if (value_item.type() != json_spirit::obj_type) {
									TraceEvent(sev, "SchemaMismatch").detail("path", kpath + format("[%d]",index)).detail("ValueType", value_item.type());
									ok = false;
									continue;
								}
								if (!schemaMatch(schema_obj, value_item.get_obj(), sev, kpath + format("[%d]", index), spath + "[0]"))
									ok = false;
								index++;
							}
						} else
							ASSERT(false);  // Schema doesn't make sense
					} else if (rv.type() == json_spirit::obj_type) {
						auto& schema_obj = sv.get_obj();
						auto& value_obj = rv.get_obj();
						if (!schemaMatch(schema_obj, value_obj, sev, kpath, spath))
							ok = false;
					}
				}
			}
			return ok;
		} catch (std::exception& e) {
			TraceEvent(SevError, "SchemaMatchException").detail("What", e.what()).detail("Path", path).detail("SchemaPath", schema_path);
			throw unknown_error();
		}
	}

	ACTOR Future<Void> fetcher(Reference<ClusterConnectionFile> connFile, StatusWorkload *self) {
		state double lastTime = now();

		loop{
			Void _ = wait(poisson(&lastTime, 1.0 / self->requestsPerSecond));
			try {
				// Since we count the requests that start, we could potentially never really hear back?
				++self->requests;
				state double issued = now();
				StatusObject result = wait(StatusClient::statusFetcher(connFile));
				++self->replies;
				BinaryWriter br(AssumeVersion(currentProtocolVersion));
				save(br, result);
				self->totalSize += br.getLength();
				TraceEvent("StatusWorkloadReply").detail("ReplySize", br.getLength()).detail("Latency", now() - issued);//.detail("Reply", json_spirit::write_string(json_spirit::mValue(result)));

				if (self->statusSchema.present() && !schemaMatch(self->statusSchema.get(), result) )
					TraceEvent(SevError, "StatusWorkloadValidationFailed").detail("JSON", json_spirit::write_string(json_spirit::mValue(result)));
			}
			catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent(SevError, "StatusWorkloadError").error(e);
					++self->errors;
				}
				throw;
			}
		}
	}

};

WorkloadFactory<StatusWorkload> StatusWorkloadFactory("Status");

TEST_CASE("fdbserver/status/schema/basic") {
	json_spirit::mValue schema;
	json_spirit::read_string( std::string("{\"apple\":3,\"banana\":\"foo\",\"sub\":{\"thing\":true},\"arr\":[{\"a\":1,\"b\":2}],\"en\":{\"$enum\":[\"foo\",\"bar\"]},\"mapped\":{\"$map\":{\"x\":true}}"), schema );

	auto check = [&schema](bool expect_ok, std::string t) {
		json_spirit::mValue test;
		json_spirit::read_string( t, test );
		TraceEvent("SchemaMatch").detail("Schema", json_spirit::write_string(schema)).detail("Value", json_spirit::write_string(test)).detail("Expect", expect_ok);
		ASSERT( expect_ok == StatusWorkload::schemaMatch(schema.get_obj(), test.get_obj(), expect_ok ? SevError : SevInfo) );
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