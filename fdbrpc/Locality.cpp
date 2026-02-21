/*
 * Locality.cpp
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

#include "fdbrpc/Locality.h"

#include <cstdio>

const UID LocalityData::UNSET_ID = UID(0x0ccb4e0feddb5583, 0x010f6b77d9d10ece);
alignas(8) const StringRef LocalityData::keyProcessId = "processid"_sr;
alignas(8) const StringRef LocalityData::keyZoneId = "zoneid"_sr;
alignas(8) const StringRef LocalityData::keyDcId = "dcid"_sr;
alignas(8) const StringRef LocalityData::keyMachineId = "machineid"_sr;
alignas(8) const StringRef LocalityData::keyDataHallId = "data_hall"_sr;
alignas(8) const StringRef LocalityData::ExcludeLocalityPrefix = "locality_"_sr;

ProcessClass::ProcessClass(std::string s, ClassSource source) : _source(source) {
	if (s == "storage")
		_class = StorageClass;
	else if (s == "transaction")
		_class = TransactionClass;
	else if (s == "resolution")
		_class = ResolutionClass;
	else if (s == "commit_proxy")
		_class = CommitProxyClass;
	else if (s == "proxy") {
		_class = CommitProxyClass;
		printf("WARNING: 'proxy' machine class is deprecated and will be automatically converted "
		       "'commit_proxy' machine class. Please use 'grv_proxy' or 'commit_proxy' specifically\n");
	} else if (s == "grv_proxy")
		_class = GrvProxyClass;
	else if (s == "master")
		_class = MasterClass;
	else if (s == "test")
		_class = TesterClass;
	else if (s == "unset")
		_class = UnsetClass;
	else if (s == "stateless")
		_class = StatelessClass;
	else if (s == "log")
		_class = LogClass;
	else if (s == "router")
		_class = LogRouterClass;
	else if (s == "cluster_controller")
		_class = ClusterControllerClass;
	else if (s == "fast_restore")
		_class = FastRestoreClass;
	else if (s == "data_distributor")
		_class = DataDistributorClass;
	else if (s == "coordinator")
		_class = CoordinatorClass;
	else if (s == "ratekeeper")
		_class = RatekeeperClass;
	else if (s == "consistency_scan")
		_class = ConsistencyScanClass;
	else if (s == "blob_manager")
		_class = BlobManagerClass;
	else if (s == "blob_worker")
		_class = BlobWorkerClass;
	else if (s == "backup")
		_class = BackupClass;
	else if (s == "encrypt_key_proxy")
		_class = EncryptKeyProxyClass;
	else if (s == "sim_http_server")
		_class = SimHTTPServerClass;
	else
		_class = InvalidClass;
}

ProcessClass::ProcessClass(std::string classStr, std::string sourceStr) {
	if (classStr == "storage")
		_class = StorageClass;
	else if (classStr == "transaction")
		_class = TransactionClass;
	else if (classStr == "resolution")
		_class = ResolutionClass;
	else if (classStr == "commit_proxy")
		_class = CommitProxyClass;
	else if (classStr == "proxy") {
		_class = CommitProxyClass;
		printf("WARNING: 'proxy' machine class is deprecated and will be automatically converted "
		       "'commit_proxy' machine class. Please use 'grv_proxy' or 'commit_proxy' specifically\n");
	} else if (classStr == "grv_proxy")
		_class = GrvProxyClass;
	else if (classStr == "master")
		_class = MasterClass;
	else if (classStr == "test")
		_class = TesterClass;
	else if (classStr == "unset")
		_class = UnsetClass;
	else if (classStr == "stateless")
		_class = StatelessClass;
	else if (classStr == "log")
		_class = LogClass;
	else if (classStr == "router")
		_class = LogRouterClass;
	else if (classStr == "cluster_controller")
		_class = ClusterControllerClass;
	else if (classStr == "fast_restore")
		_class = FastRestoreClass;
	else if (classStr == "data_distributor")
		_class = DataDistributorClass;
	else if (classStr == "coordinator")
		_class = CoordinatorClass;
	else if (classStr == "ratekeeper")
		_class = RatekeeperClass;
	else if (classStr == "consistency_scan")
		_class = ConsistencyScanClass;
	else if (classStr == "blob_manager")
		_class = BlobManagerClass;
	else if (classStr == "blob_worker")
		_class = BlobWorkerClass;
	else if (classStr == "backup")
		_class = BackupClass;
	else if (classStr == "encrypt_key_proxy")
		_class = EncryptKeyProxyClass;
	else if (classStr == "sim_http_server")
		_class = SimHTTPServerClass;
	else
		_class = InvalidClass;

	if (sourceStr == "command_line")
		_source = CommandLineSource;
	else if (sourceStr == "configure_auto")
		_source = AutoSource;
	else if (sourceStr == "set_class")
		_source = DBSource;
	else
		_source = InvalidSource;
}

std::string ProcessClass::toString() const {
	switch (_class) {
	case UnsetClass:
		return "unset";
	case StorageClass:
		return "storage";
	case TransactionClass:
		return "transaction";
	case ResolutionClass:
		return "resolution";
	case CommitProxyClass:
		return "commit_proxy";
	case GrvProxyClass:
		return "grv_proxy";
	case MasterClass:
		return "master";
	case TesterClass:
		return "test";
	case StatelessClass:
		return "stateless";
	case LogClass:
		return "log";
	case LogRouterClass:
		return "router";
	case ClusterControllerClass:
		return "cluster_controller";
	case FastRestoreClass:
		return "fast_restore";
	case DataDistributorClass:
		return "data_distributor";
	case CoordinatorClass:
		return "coordinator";
	case RatekeeperClass:
		return "ratekeeper";
	case ConsistencyScanClass:
		return "consistency_scan";
	case BlobManagerClass:
		return "blob_manager";
	case BlobWorkerClass:
		return "blob_worker";
	case BackupClass:
		return "backup";
	case EncryptKeyProxyClass:
		return "encrypt_key_proxy";
	case SimHTTPServerClass:
		return "sim_http_server";
	default:
		return "invalid";
	}
}

std::string ProcessClass::sourceString() const {
	switch (_source) {
	case CommandLineSource:
		return "command_line";
	case AutoSource:
		return "configure_auto";
	case DBSource:
		return "set_class";
	default:
		return "invalid";
	}
}

ProcessClass::Fitness ProcessClass::machineClassFitness(ClusterRole role) const {
	switch (role) {
	case ProcessClass::Storage:
		switch (_class) {
		case ProcessClass::StorageClass:
			return ProcessClass::BestFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::TransactionClass:
		case ProcessClass::LogClass:
			return ProcessClass::WorstFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
		default:
			return ProcessClass::NeverAssign;
		}
	case ProcessClass::TLog:
		switch (_class) {
		case ProcessClass::LogClass:
			return ProcessClass::BestFit;
		case ProcessClass::TransactionClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::StorageClass:
			return ProcessClass::WorstFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::NeverAssign;
		}
	case ProcessClass::CommitProxy: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (_class) {
		case ProcessClass::CommitProxyClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::TransactionClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::GrvProxy: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (_class) {
		case ProcessClass::GrvProxyClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::TransactionClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::Master: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (_class) {
		case ProcessClass::MasterClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::TransactionClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::Resolver: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (_class) {
		case ProcessClass::ResolutionClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::TransactionClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::LogRouter:
		switch (_class) {
		case ProcessClass::LogRouterClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::TransactionClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::Backup:
		switch (_class) {
		case ProcessClass::BackupClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
		case ProcessClass::LogRouterClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::TransactionClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::MasterClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::ClusterController:
		switch (_class) {
		case ProcessClass::ClusterControllerClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::MasterClass:
		case ProcessClass::ResolutionClass:
		case ProcessClass::TransactionClass:
		case ProcessClass::CommitProxyClass:
		case ProcessClass::GrvProxyClass:
		case ProcessClass::LogRouterClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::DataDistributor:
		switch (_class) {
		case ProcessClass::DataDistributorClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::MasterClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::Ratekeeper:
		switch (_class) {
		case ProcessClass::RatekeeperClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::MasterClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::ConsistencyScan:
		switch (_class) {
		case ProcessClass::ConsistencyScanClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::MasterClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::BlobManager:
		switch (_class) {
		case ProcessClass::BlobManagerClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::MasterClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	case ProcessClass::BlobWorker:
		switch (_class) {
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::BestFit;
		default:
			return ProcessClass::NeverAssign;
		}
	case ProcessClass::BlobMigrator:
		switch (_class) {
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::MasterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::OkayFit;
		default:
			return ProcessClass::NeverAssign;
		}
	case ProcessClass::EncryptKeyProxy:
		switch (_class) {
		case ProcessClass::EncryptKeyProxyClass:
			return ProcessClass::BestFit;
		case ProcessClass::StatelessClass:
			return ProcessClass::GoodFit;
		case ProcessClass::UnsetClass:
			return ProcessClass::UnsetFit;
		case ProcessClass::MasterClass:
			return ProcessClass::OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return ProcessClass::NeverAssign;
		default:
			return ProcessClass::WorstFit;
		}
	default:
		return ProcessClass::NeverAssign;
	}
}

LBDistance::Type loadBalanceDistance(LocalityData const& loc1, LocalityData const& loc2, NetworkAddress const& addr2) {
	if (FLOW_KNOBS->LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED && loc1.zoneId().present() &&
	    loc1.zoneId() == loc2.zoneId()) {
		return LBDistance::SAME_MACHINE;
	}
	// FIXME: add this back in when load balancing works with local requests
	// if ( g_network->isAddressOnThisHost( addr2 ) )
	//	return LBDistance::SAME_MACHINE;
	if (FLOW_KNOBS->LOAD_BALANCE_DC_ID_LOCALITY_ENABLED && loc1.dcId().present() && loc1.dcId() == loc2.dcId()) {
		return LBDistance::SAME_DC;
	}
	return LBDistance::DISTANT;
}
