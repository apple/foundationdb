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

FDB_DEFINE_BOOLEAN_PARAM(ApplyManagementClusterUpdates);
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

KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())>&
MetaclusterMetadata::metaclusterRegistration() {
	static KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())> instance(
	    "\xff/metacluster/clusterRegistration"_sr, IncludeVersion());
	return instance;
}