/*
 * StatusSchema.cpp
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

#include "fdbclient/StatusSchema.h"

#include <exception>
#include <map>
#include <set>

#include "fdbclient/json_spirit/json_spirit_writer_template.h"
#include "flow/Error.h"
#include "fmt/format.h"

namespace {

json_spirit::Value_type normJSONType(json_spirit::Value_type type) {
	if (type == json_spirit::int_type)
		return json_spirit::real_type;
	return type;
}

} // namespace

void schemaCoverage(std::string const& spath, bool covered) {
	static std::map<bool, std::set<std::string>> coveredSchemaPaths;

	if (coveredSchemaPaths[covered].insert(spath).second) {
		TraceEvent ev(SevInfo, "CodeCoverage");
		ev.detail("File", "documentation/StatusSchema.json/" + spath).detail("Line", 0);
		if (!covered)
			ev.detail("Covered", 0);
	}
}

bool schemaMatch(json_spirit::mValue const& schemaValue,
                 json_spirit::mValue const& resultValue,
                 std::string& errorStr,
                 Severity sev,
                 bool checkCoverage,
                 std::string path,
                 std::string schemaPath) {
	// Returns true if everything in `result` is permitted by `schema`
	bool ok = true;

	try {
		if (normJSONType(schemaValue.type()) != normJSONType(resultValue.type())) {
			errorStr += format("ERROR: Incorrect value type for key `%s'\n", path.c_str());
			TraceEvent(sev, "SchemaMismatch")
			    .detail("Path", path)
			    .detail("SchemaType", schemaValue.type())
			    .detail("ValueType", resultValue.type());
			return false;
		}

		if (resultValue.type() == json_spirit::obj_type) {
			auto& result = resultValue.get_obj();
			auto& schema = schemaValue.get_obj();

			for (auto& rkv : result) {
				auto& key = rkv.first;
				auto& rv = rkv.second;
				std::string kpath = path + "." + key;
				std::string spath = schemaPath + "." + key;

				if (checkCoverage) {
					schemaCoverage(spath);
				}

				if (!schema.contains(key)) {
					errorStr += format("ERROR: Unknown key `%s'\n", kpath.c_str());
					TraceEvent(sev, "SchemaMismatch").detail("Path", kpath).detail("SchemaPath", spath);
					ok = false;
					continue;
				}
				auto& sv = schema.at(key);

				if (sv.type() == json_spirit::obj_type && sv.get_obj().contains("$enum")) {
					auto& enum_values = sv.get_obj().at("$enum").get_array();

					bool any_match = false;
					for (auto& enum_item : enum_values)
						if (enum_item == rv) {
							any_match = true;
							if (checkCoverage) {
								schemaCoverage(spath + ".$enum." + enum_item.get_str());
							}
							break;
						}
					if (!any_match) {
						errorStr += format("ERROR: Unknown value `%s' for key `%s'\n",
						                   json_spirit::write_string(rv).c_str(),
						                   kpath.c_str());
						TraceEvent(sev, "SchemaMismatch")
						    .detail("Path", kpath)
						    .detail("SchemaEnumItems", enum_values.size())
						    .detail("Value", json_spirit::write_string(rv));
						if (checkCoverage) {
							schemaCoverage(spath + ".$enum." + json_spirit::write_string(rv));
						}
						ok = false;
					}
				} else if (sv.type() == json_spirit::obj_type && sv.get_obj().contains("$map")) {
					if (rv.type() != json_spirit::obj_type) {
						errorStr += format("ERROR: Expected an object as the value for key `%s'\n", kpath.c_str());
						TraceEvent(sev, "SchemaMismatch")
						    .detail("Path", kpath)
						    .detail("SchemaType", sv.type())
						    .detail("ValueType", rv.type());
						ok = false;
						continue;
					}
					if (sv.get_obj().at("$map").type() != json_spirit::obj_type) {
						continue;
					}
					auto& schemaVal = sv.get_obj().at("$map");
					auto& valueObj = rv.get_obj();

					if (checkCoverage) {
						schemaCoverage(spath + ".$map");
					}

					for (auto& valuePair : valueObj) {
						auto vpath = kpath + "[" + valuePair.first + "]";
						auto upath = spath + ".$map";
						if (valuePair.second.type() != json_spirit::obj_type) {
							errorStr += format("ERROR: Expected an object for `%s'\n", vpath.c_str());
							TraceEvent(sev, "SchemaMismatch")
							    .detail("Path", vpath)
							    .detail("ValueType", valuePair.second.type());
							ok = false;
							continue;
						}
						if (!schemaMatch(schemaVal, valuePair.second, errorStr, sev, checkCoverage, vpath, upath)) {
							ok = false;
						}
					}
				} else {
					if (!schemaMatch(sv, rv, errorStr, sev, checkCoverage, kpath, spath)) {
						ok = false;
					}
				}
			}
		} else if (resultValue.type() == json_spirit::array_type) {
			auto& valueArray = resultValue.get_array();
			auto& schemaArray = schemaValue.get_array();
			if (schemaArray.empty()) {
				// An empty schema array means that the value array is required to be empty
				if (!valueArray.empty()) {
					errorStr += format("ERROR: Expected an empty array for key `%s'\n", path.c_str());
					TraceEvent(sev, "SchemaMismatch")
					    .detail("Path", path)
					    .detail("SchemaSize", schemaArray.size())
					    .detail("ValueSize", valueArray.size());
					return false;
				}
			} else if (schemaArray.size() == 1) {
				// A one item schema array means that all items in the value must match the first item in the schema
				int index = 0;
				for (auto& valueItem : valueArray) {
					if (!schemaMatch(schemaArray[0],
					                 valueItem,
					                 errorStr,
					                 sev,
					                 checkCoverage,
					                 path + format("[%d]", index),
					                 schemaPath + "[0]")) {
						ok = false;
					}
					index++;
				}
			} else {
				ASSERT(false); // Schema doesn't make sense
			}
		}
		return ok;
	} catch (std::exception& e) {
		TraceEvent(SevError, "SchemaMatchException")
		    .detail("What", e.what())
		    .detail("Path", path)
		    .detail("SchemaPath", schemaPath);
		throw unknown_error();
	}
}
