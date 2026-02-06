/*
 * ConfigureCommand.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.Unless
 *
 * 0 required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "fdbctl/ControlCommands.h"
#include "fdbctl/control_service/control_service.pb.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/Arena.h"
#include "fmt/format.h"
#include <grpcpp/support/status.h>

namespace fdbctl {
ConfigureReply::Result configurationResultToProto(ConfigurationResult result) {
	switch (result) {
	case ConfigurationResult::SUCCESS:
		return ConfigureReply::SUCCESS;
	case ConfigurationResult::NO_OPTIONS_PROVIDED:
		return ConfigureReply::NO_OPTIONS_PROVIDED;
	case ConfigurationResult::CONFLICTING_OPTIONS:
		return ConfigureReply::CONFLICTING_OPTIONS;
	case ConfigurationResult::UNKNOWN_OPTION:
		return ConfigureReply::UNKNOWN_OPTION;
	case ConfigurationResult::INCOMPLETE_CONFIGURATION:
		return ConfigureReply::INCOMPLETE_CONFIGURATION;
	case ConfigurationResult::INVALID_CONFIGURATION:
		return ConfigureReply::INVALID_CONFIGURATION;
	case ConfigurationResult::STORAGE_MIGRATION_DISABLED:
		return ConfigureReply::STORAGE_MIGRATION_DISABLED;
	case ConfigurationResult::DATABASE_ALREADY_CREATED:
		return ConfigureReply::DATABASE_ALREADY_CREATED;
	case ConfigurationResult::DATABASE_CREATED:
		return ConfigureReply::DATABASE_CREATED;
	case ConfigurationResult::DATABASE_UNAVAILABLE:
		return ConfigureReply::DATABASE_UNAVAILABLE;
	case ConfigurationResult::STORAGE_IN_UNKNOWN_DCID:
		return ConfigureReply::STORAGE_IN_UNKNOWN_DCID;
	case ConfigurationResult::REGION_NOT_FULLY_REPLICATED:
		return ConfigureReply::REGION_NOT_FULLY_REPLICATED;
	case ConfigurationResult::MULTIPLE_ACTIVE_REGIONS:
		return ConfigureReply::MULTIPLE_ACTIVE_REGIONS;
	case ConfigurationResult::REGIONS_CHANGED:
		return ConfigureReply::REGIONS_CHANGED;
	case ConfigurationResult::NOT_ENOUGH_WORKERS:
		return ConfigureReply::NOT_ENOUGH_WORKERS;
	case ConfigurationResult::REGION_REPLICATION_MISMATCH:
		return ConfigureReply::REGION_REPLICATION_MISMATCH;
	case ConfigurationResult::DCID_MISSING:
		return ConfigureReply::DCID_MISSING;
	case ConfigurationResult::LOCKED_NOT_NEW:
		return ConfigureReply::LOCKED_NOT_NEW;
	case ConfigurationResult::SUCCESS_WARN_PPW_GRADUAL:
		return ConfigureReply::SUCCESS_WARN_PPW_GRADUAL;
	case ConfigurationResult::SUCCESS_WARN_SHARDED_ROCKSDB_EXPERIMENTAL:
		return ConfigureReply::SUCCESS_WARN_SHARDED_ROCKSDB_EXPERIMENTAL;
	case ConfigurationResult::DATABASE_IS_REGISTERED:
		return ConfigureReply::DATABASE_IS_REGISTERED;
	case ConfigurationResult::ENCRYPTION_AT_REST_MODE_ALREADY_SET:
		return ConfigureReply::ENCRYPTION_AT_REST_MODE_ALREADY_SET;
	case ConfigurationResult::INVALID_STORAGE_TYPE:
		return ConfigureReply::INVALID_STORAGE_TYPE;
	default:
		return ConfigureReply::UNKNOWN_OPTION;
	}
}

Future<grpc::Status> configureAuto(Reference<IDatabase> db,
                                   const ConfigureAutoSuggestRequest* req,
                                   ConfigureAutoSuggestReply* rep) {

	return grpc::Status::OK;
}

Future<grpc::Status> configure(Reference<IDatabase> db, const ConfigureRequest* req, ConfigureReply* rep) {
	// Build configuration tokens from structured fields
	std::vector<std::string> tokens;

	// Handle new database creation
	if (req->has_new_database() && req->new_database()) {
		tokens.push_back("new");
	}

	// handle redundancy mode
	if (req->has_redundancy_mode()) {
		switch (req->redundancy_mode()) {
		case ConfigureRequest::SINGLE:
			tokens.push_back("single");
			break;
		case ConfigureRequest::DOUBLE:
			tokens.push_back("double");
			break;
		case ConfigureRequest::TRIPLE:
			tokens.push_back("triple");
			break;
		case ConfigureRequest::THREE_DATA_HALL:
			tokens.push_back("three_data_hall");
			break;
		case ConfigureRequest::THREE_DATA_HALL_FALLBACK:
			tokens.push_back("three_data_hall_fallback");
			break;
		case ConfigureRequest::THREE_DATACENTER:
			tokens.push_back("three_datacenter");
			break;
		default:
			break;
		}
	}

	// Handle storage engine
	if (req->has_storage_engine()) {
		switch (req->storage_engine()) {
		case ConfigureRequest::SSD_BTREE_V1:
			tokens.push_back("ssd-1");
			break;
		case ConfigureRequest::SSD_BTREE_V2:
			tokens.push_back("ssd-2");
			break;
		case ConfigureRequest::SSD:
			tokens.push_back("ssd");
			break;
		case ConfigureRequest::SSD_REDWOOD_V1:
			tokens.push_back("ssd-redwood-1");
			break;
		case ConfigureRequest::SSD_ROCKSDB_V1:
			tokens.push_back("ssd-rocksdb-v1");
			break;
		case ConfigureRequest::SSD_SHARDED_ROCKSDB:
			tokens.push_back("ssd-sharded-rocksdb");
			break;
		case ConfigureRequest::MEMORY:
			tokens.push_back("memory");
			break;
		case ConfigureRequest::MEMORY_RADIXTREE:
			tokens.push_back("memory-radixtree");
			break;
		default:
			break;
		}
	}

	// Handle process counts
	if (req->has_roles_counts()) {
		const auto& roles = req->roles_counts();
		if (roles.has_logs()) {
			tokens.push_back(fmt::format("logs={}", roles.logs()));
		}
		if (roles.has_commit_proxies()) {
			tokens.push_back(fmt::format("commit_proxies={}", roles.commit_proxies()));
		}
		if (roles.has_grv_proxies()) {
			tokens.push_back(fmt::format("grv_proxies={}", roles.grv_proxies()));
			std::cout << "Added: " << tokens.back() << std::endl;
		}
		if (roles.has_resolvers()) {
			tokens.push_back(fmt::format("resolvers={}", roles.resolvers()));
		}
	}

	// Handle perpetual storage wiggle configuration
	if (req->has_perpetual_storage_wiggle_config()) {
		const auto& wiggle_config = req->perpetual_storage_wiggle_config();

		if (wiggle_config.has_enable()) {
			tokens.push_back(fmt::format("perpetual_storage_wiggle={}", wiggle_config.enable() ? 1 : 0));
		}

		if (wiggle_config.has_locality_filter()) {
			tokens.push_back(fmt::format("perpetual_storage_wiggle_locality={}", wiggle_config.locality_filter()));
		}

		if (wiggle_config.has_storage_engine()) {
			// Map storage engine enum to string
			std::string engine_str;
			switch (wiggle_config.storage_engine()) {
			case ConfigureRequest::SSD_BTREE_V1:
				engine_str = "ssd-1";
				break;
			case ConfigureRequest::SSD_BTREE_V2:
			case ConfigureRequest::SSD:
				engine_str = "ssd-2";
				break;
			case ConfigureRequest::SSD_REDWOOD_V1:
				engine_str = "ssd-redwood-1";
				break;
			case ConfigureRequest::SSD_ROCKSDB_V1:
				engine_str = "ssd-rocksdb-v1";
				break;
			case ConfigureRequest::SSD_SHARDED_ROCKSDB:
				engine_str = "ssd-sharded-rocksdb";
				break;
			case ConfigureRequest::MEMORY:
				engine_str = "memory";
				break;
			case ConfigureRequest::MEMORY_RADIXTREE:
				engine_str = "memory-radixtree";
				break;
			default:
				break;
			}
			if (!engine_str.empty()) {
				tokens.push_back(fmt::format("perpetual_storage_wiggle_engine={}", engine_str));
			}
		}

		if (wiggle_config.has_storage_migration_type()) {
			switch (wiggle_config.storage_migration_type()) {
			case ConfigureRequest::PerpetualStorageWiggleConfig::DISABLED:
				tokens.push_back("storage_migration_type=disabled");
				break;
			case ConfigureRequest::PerpetualStorageWiggleConfig::GRADUAL:
				tokens.push_back("storage_migration_type=gradual");
				break;
			case ConfigureRequest::PerpetualStorageWiggleConfig::AGGRESSIVE:
				tokens.push_back("storage_migration_type=aggressive");
				break;
			default:
				break;
			}
		}
	}

	// Handle encryption at rest mode
	if (req->has_encryption_at_rest_mode()) {
		switch (req->encryption_at_rest_mode()) {
		case ConfigureRequest::DISABLED_ENCRYPTION:
			tokens.push_back("encryption_at_rest_mode=disabled");
			break;
		case ConfigureRequest::CLUSTER_AWARE:
			tokens.push_back("encryption_at_rest_mode=cluster_aware");
			break;
		default:
			break;
		}
	}

	// Handle excluded addresses
	if (req->exclude_addresses_size() > 0) {
		std::string exclude_list;
		for (int i = 0; i < req->exclude_addresses_size(); ++i) {
			if (i > 0)
				exclude_list += ",";
			exclude_list += req->exclude_addresses(i);
		}
		tokens.push_back(fmt::format("exclude={}", exclude_list));
	}

	// Handle TSS configuration
	if (req->has_tss_config()) {
		if (req->tss_config().has_enable_tss() && req->tss_config().enable_tss()) {
			tokens.push_back("tss");
		}
		if (req->tss_config().has_tss_count()) {
			tokens.push_back(fmt::format("count={}", req->tss_config().tss_count()));
		}
	}

	// Check if we have any configuration options
	if (tokens.empty()) {
		rep->set_result(ConfigureReply::NO_OPTIONS_PROVIDED);
		rep->set_message("No configuration options provided");
		co_return grpc::Status::OK;
	}

	bool force = req->has_force() ? req->force() : false;

	try {
		Optional<ConfigureAutoResult> conf;
		std::vector<StringRef> token_refs;
		Arena ar;
		for (auto t : tokens) {
			token_refs.push_back(StringRef(ar, t));
		}

		ConfigurationResult result = co_await ManagementAPI::changeConfig(db, token_refs, conf, force);
		rep->set_result(configurationResultToProto(result));

		// Set appropriate messages for different results
		switch (result) {
		case ConfigurationResult::SUCCESS:
			rep->set_message("Configuration changed");
			break;
		case ConfigurationResult::DATABASE_CREATED:
			rep->set_message("Database created");
			break;
		case ConfigurationResult::SUCCESS_WARN_PPW_GRADUAL:
			rep->set_message("Configuration changed, with warnings about perpetual wiggle");
			break;
		case ConfigurationResult::SUCCESS_WARN_SHARDED_ROCKSDB_EXPERIMENTAL:
			rep->set_message("Configuration changed, sharded RocksDB is experimental");
			break;
		case ConfigurationResult::DATABASE_UNAVAILABLE:
			rep->set_message("Database is unavailable. Use FORCE to configure without this check");
			break;
		case ConfigurationResult::NOT_ENOUGH_WORKERS:
			rep->set_message("Not enough processes exist to support the specified configuration. Use FORCE to "
			                 "configure without this check");
			break;
		case ConfigurationResult::INVALID_CONFIGURATION:
			rep->set_message("These changes would make the configuration invalid");
			break;
		default:
			// For other errors, provide a generic message
			rep->set_message("Configuration change failed");
			break;
		}

		co_return grpc::Status::OK;
	} catch (Error& e) {
		co_return grpc::Status(grpc::StatusCode::INTERNAL, fmt::format("Configuration failed: {}", e.name()));
	}
}
} // namespace fdbctl
