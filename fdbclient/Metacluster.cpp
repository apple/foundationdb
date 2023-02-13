/*
 * Metacluster.cpp
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

#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "libb64/decode.h"
#include "libb64/encode.h"

FDB_DEFINE_BOOLEAN_PARAM(AddNewTenants);
FDB_DEFINE_BOOLEAN_PARAM(RemoveMissingTenants);
FDB_DEFINE_BOOLEAN_PARAM(AssignClusterAutomatically);

std::string clusterTypeToString(const ClusterType& clusterType) {
	switch (clusterType) {
	case ClusterType::STANDALONE:
		return "standalone";
	case ClusterType::METACLUSTER_MANAGEMENT:
		return "metacluster_management";
	case ClusterType::METACLUSTER_DATA:
		return "metacluster_data";
	default:
		return "unknown";
	}
}

std::string DataClusterEntry::clusterStateToString(DataClusterState clusterState) {
	switch (clusterState) {
	case DataClusterState::READY:
		return "ready";
	case DataClusterState::REMOVING:
		return "removing";
	case DataClusterState::RESTORING:
		return "restoring";
	default:
		UNREACHABLE();
	}
}

DataClusterState DataClusterEntry::stringToClusterState(std::string stateStr) {
	if (stateStr == "ready") {
		return DataClusterState::READY;
	} else if (stateStr == "removing") {
		return DataClusterState::REMOVING;
	} else if (stateStr == "restoring") {
		return DataClusterState::RESTORING;
	}

	UNREACHABLE();
}

json_spirit::mObject DataClusterEntry::toJson() const {
	json_spirit::mObject obj;
	obj["capacity"] = capacity.toJson();
	obj["allocated"] = allocated.toJson();
	obj["cluster_state"] = DataClusterEntry::clusterStateToString(clusterState);
	return obj;
}

json_spirit::mObject ClusterUsage::toJson() const {
	json_spirit::mObject obj;
	obj["num_tenant_groups"] = numTenantGroups;
	return obj;
}

TenantMapEntry::TenantMapEntry(MetaclusterTenantMapEntry metaclusterEntry)
  : tenantName(metaclusterEntry.tenantName), tenantLockState(metaclusterEntry.tenantLockState),
    tenantGroup(metaclusterEntry.tenantGroup), configurationSequenceNum(metaclusterEntry.configurationSequenceNum) {
	setId(metaclusterEntry.id);
}

MetaclusterTenantMapEntry::MetaclusterTenantMapEntry() {}
MetaclusterTenantMapEntry::MetaclusterTenantMapEntry(int64_t id,
                                                     TenantName tenantName,
                                                     TenantAPI::TenantState tenantState)
  : tenantName(tenantName), tenantState(tenantState) {
	setId(id);
}
MetaclusterTenantMapEntry::MetaclusterTenantMapEntry(int64_t id,
                                                     TenantName tenantName,
                                                     TenantAPI::TenantState tenantState,
                                                     Optional<TenantGroupName> tenantGroup)
  : tenantName(tenantName), tenantState(tenantState), tenantGroup(tenantGroup) {
	setId(id);
}

void MetaclusterTenantMapEntry::setId(int64_t id) {
	ASSERT(id >= 0);
	this->id = id;
	prefix = TenantAPI::idToPrefix(id);
}

std::string MetaclusterTenantMapEntry::toJson() const {
	json_spirit::mObject tenantEntry;
	tenantEntry["id"] = id;

	tenantEntry["name"] = binaryToJson(tenantName);
	tenantEntry["prefix"] = binaryToJson(prefix);

	tenantEntry["tenant_state"] = TenantAPI::tenantStateToString(tenantState);
	tenantEntry["assigned_cluster"] = binaryToJson(assignedCluster);

	if (tenantGroup.present()) {
		tenantEntry["tenant_group"] = binaryToJson(tenantGroup.get());
	}

	tenantEntry["lock_state"] = TenantAPI::tenantLockStateToString(tenantLockState);
	if (tenantState == TenantAPI::TenantState::RENAMING) {
		ASSERT(renameDestination.present());
		tenantEntry["rename_destination"] = binaryToJson(renameDestination.get());
	} else if (tenantState == TenantAPI::TenantState::ERROR) {
		tenantEntry["error"] = error;
	}

	return json_spirit::write_string(json_spirit::mValue(tenantEntry));
}

bool MetaclusterTenantMapEntry::matchesConfiguration(MetaclusterTenantMapEntry const& other) const {
	return tenantName == other.tenantName && tenantLockState == other.tenantLockState &&
	       tenantGroup == other.tenantGroup && assignedCluster == other.assignedCluster;
}

void MetaclusterTenantMapEntry::configure(Standalone<StringRef> parameter, Optional<Value> value) {
	if (parameter == "tenant_group"_sr) {
		tenantGroup = value;
	} else if (parameter == "assigned_cluster"_sr && value.present()) {
		assignedCluster = value.get();
	} else {
		TraceEvent(SevWarnAlways, "UnknownTenantConfigurationParameter").detail("Parameter", parameter);
		throw invalid_tenant_configuration();
	}
}

KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())>&
MetaclusterMetadata::metaclusterRegistration() {
	static KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())> instance(
	    "\xff/metacluster/clusterRegistration"_sr, IncludeVersion());
	return instance;
}