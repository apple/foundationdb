/*
 * LatencyBandConfig.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/LatencyBandConfig.h"

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"

bool operator==(LatencyBandConfig::RequestConfig const& lhs, LatencyBandConfig::RequestConfig const& rhs) {
	return typeid(lhs) == typeid(rhs) && lhs.isEqual(rhs);
}

bool operator!=(LatencyBandConfig::RequestConfig const& lhs, LatencyBandConfig::RequestConfig const& rhs) {
	return !(lhs == rhs);
}

bool LatencyBandConfig::RequestConfig::isEqual(RequestConfig const& r) const {
	return bands == r.bands;
};

void LatencyBandConfig::RequestConfig::fromJson(JSONDoc json) {
	json_spirit::mArray bandsArray;
	if (json.get("bands", bandsArray)) {
		for (auto b : bandsArray) {
			bands.insert(b.get_real());
		}
	}
}

void LatencyBandConfig::ReadConfig::fromJson(JSONDoc json) {
	RequestConfig::fromJson(json);

	int value;
	if (json.get("max_read_bytes", value)) {
		maxReadBytes = value;
	}
	if (json.get("max_key_selector_offset", value)) {
		maxKeySelectorOffset = value;
	}
}

bool LatencyBandConfig::ReadConfig::isEqual(RequestConfig const& r) const {
	ReadConfig const& other = static_cast<ReadConfig const&>(r);
	return RequestConfig::isEqual(r) && maxReadBytes == other.maxReadBytes &&
	       maxKeySelectorOffset == other.maxKeySelectorOffset;
}

void LatencyBandConfig::CommitConfig::fromJson(JSONDoc json) {
	RequestConfig::fromJson(json);

	int value;
	if (json.get("max_commit_bytes", value)) {
		maxCommitBytes = value;
	}
}

bool LatencyBandConfig::CommitConfig::isEqual(RequestConfig const& r) const {
	CommitConfig const& other = static_cast<CommitConfig const&>(r);
	return RequestConfig::isEqual(r) && maxCommitBytes == other.maxCommitBytes;
}

Optional<LatencyBandConfig> LatencyBandConfig::parse(ValueRef configurationString) {
	Optional<LatencyBandConfig> config;
	if (configurationString.size() == 0) {
		return config;
	}

	json_spirit::mValue parsedConfig;
	if (!json_spirit::read_string(configurationString.toString(), parsedConfig)) {
		TraceEvent(SevWarnAlways, "InvalidLatencyBandConfiguration")
		    .detail("Reason", "InvalidJSON")
		    .detail("Configuration", configurationString);
		return config;
	}

	json_spirit::mObject configJson = parsedConfig.get_obj();

	json_spirit::mValue schema;
	if (!json_spirit::read_string(JSONSchemas::latencyBandConfigurationSchema.toString(), schema)) {
		ASSERT(false);
	}

	std::string errorStr;
	if (!schemaMatch(schema.get_obj(), configJson, errorStr)) {
		TraceEvent(SevWarnAlways, "InvalidLatencyBandConfiguration")
		    .detail("Reason", "SchemaMismatch")
		    .detail("Configuration", configurationString)
		    .detail("Error", errorStr);
		return config;
	}

	JSONDoc configDoc(configJson);

	config = LatencyBandConfig();

	config.get().grvConfig.fromJson(configDoc.subDoc("get_read_version"));
	config.get().readConfig.fromJson(configDoc.subDoc("read"));
	config.get().commitConfig.fromJson(configDoc.subDoc("commit"));

	return config;
}

bool LatencyBandConfig::operator==(LatencyBandConfig const& r) const {
	return grvConfig == r.grvConfig && readConfig == r.readConfig && commitConfig == r.commitConfig;
}

bool LatencyBandConfig::operator!=(LatencyBandConfig const& r) const {
	return !(*this == r);
}
