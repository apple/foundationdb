/*
 * MetaclusterInternalManagementClusterSpecialKeys.actor.cpp
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

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Metacluster.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const KeyRangeRef MetaclusterInternalManagementClusterImpl::submoduleRange =
    KeyRangeRef("management_cluster/"_sr, "management_cluster0"_sr);

MetaclusterInternalManagementClusterImpl::MetaclusterInternalManagementClusterImpl(KeyRangeRef kr)
  : SpecialKeyRangeRWImpl(kr) {}

KeyRef extractCommand(ReadYourWritesTransaction* ryw,
                      KeyRef& beginKey,
                      KeyRef& endKey,
                      KeyRangeRef fullRange,
                      Optional<KeyRef> endRangeReplacement = Optional<KeyRef>()) {

	KeyRef command = beginKey.eat("/");
	KeyRef endCommand = endKey.substr(0, std::min(endKey.size(), command.size()));
	endKey = endKey.substr(endCommand.size());

	if (command != endCommand && !endKey.empty() && endKey[0] != '/' &&
	    (endKey[0] != '0' || !endRangeReplacement.present())) {
		TraceEvent(SevWarn, "InvalidMetaclusterInternalCommand")
		    .detail("Reason", "Clear spans multiple submodules")
		    .detail("Range", fullRange);
		ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
		    false, "metacluster internal management cluster", "clear spans multiple submodules"));
		throw special_keys_api_failure();
	}

	if (endKey.size() > 0 && endKey[0] == '/') {
		endKey = endKey.substr(1);
	} else {
		endKey = endRangeReplacement.get();
	}

	return command;
}

ACTOR Future<RangeResult> getDataClusterList(ReadYourWritesTransaction* ryw,
                                             KeyRangeRef kr,
                                             GetRangeLimits limitsHint) {
	std::map<ClusterName, DataClusterMetadata> clusters =
	    wait(MetaclusterAPI::managementClusterListClusters(&ryw->getTransaction(), kr.begin, kr.end, limitsHint.rows));

	RangeResult results;
	for (auto cluster : clusters) {
		ValueRef value =
		    ObjectWriter::toValue(cluster.second, IncludeVersion(ProtocolVersion::withMetacluster()), results.arena());
		results.push_back(
		    results.arena(),
		    KeyValueRef(cluster.first.withPrefix(
		                    "\xff\xff/metacluster_internal/management_cluster/data_cluster/map/"_sr, results.arena()),
		                value));
	}

	return results;
}

Future<RangeResult> MetaclusterInternalManagementClusterImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                       KeyRangeRef kr,
                                                                       GetRangeLimits limitsHint) const {

	KeyRangeRef subRange =
	    kr.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::METACLUSTER_INTERNAL).begin)
	        .removePrefix(MetaclusterInternalManagementClusterImpl::submoduleRange.begin);

	KeyRef begin = subRange.begin;
	KeyRef end = subRange.end;

	KeyRef command = extractCommand(ryw, begin, end, kr);

	if (command == "data_cluster"_sr) {
		command = extractCommand(ryw, begin, end, kr, "\xff\xff"_sr);
		if (command == "map"_sr) {
			return getDataClusterList(ryw, KeyRangeRef(begin, end), limitsHint);
		}
	}

	return RangeResult();
}

void applyDataClusterConfig(ReadYourWritesTransaction* ryw,
                            ClusterNameRef clusterName,
                            std::vector<std::pair<StringRef, Optional<Value>>> configEntries,
                            DataClusterMetadata* clusterMetadata) {
	for (auto config : configEntries) {
		if (config.first == "connection_string"_sr && config.second.present()) {
			clusterMetadata->connectionString = ClusterConnectionString(config.second.get().toString());
		} else if (config.first == "capacity.num_tenant_groups"_sr) {
			if (config.second.present()) {
				int n;
				if (sscanf(config.second.get().toString().c_str(),
				           "%d%n",
				           &clusterMetadata->entry.capacity.numTenantGroups,
				           &n) != 1 ||
				    n != config.second.get().size() || clusterMetadata->entry.capacity.numTenantGroups < 0) {

					TraceEvent(SevWarn, "InvalidDataClusterTenantGroupConfig")
					    .detail("ClusterName", clusterName)
					    .detail("Value", config.second);
					ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
					    false,
					    "set data cluster tenant group capacity",
					    format("invalid data cluster tenant group capacity `%s' for cluster `%s'",
					           config.second.get().toString().c_str(),
					           clusterName.toString().c_str())));
					throw special_keys_api_failure();
				}
			} else {
				clusterMetadata->entry.capacity.numTenantGroups = 0;
			}
		} else {
			TraceEvent(SevWarn, "InvalidDataClusterConfig")
			    .detail("ClusterName", clusterName)
			    .detail("ConfigName", config.first)
			    .detail("Value", config.second);
			ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
			    false,
			    "set data cluster configuration",
			    format("invalid data cluster configuration option `%s' for cluster `%s'",
			           config.first.toString().c_str(),
			           clusterName.toString().c_str())));
			throw special_keys_api_failure();
		}
	}
}

ACTOR Future<Void> registerDataCluster(ReadYourWritesTransaction* ryw,
                                       ClusterNameRef clusterName,
                                       std::string connectionString,
                                       Optional<std::vector<std::pair<StringRef, Optional<Value>>>> configMutations) {
	state DataClusterMetadata metadata;

	// TODO: what happens if connection string is invalid?
	metadata.connectionString = ClusterConnectionString(connectionString);

	if (configMutations.present()) {
		applyDataClusterConfig(ryw, clusterName, configMutations.get(), &metadata);
	}

	wait(MetaclusterAPI::managementClusterRegister(
	    &ryw->getTransaction(), clusterName, metadata.connectionString.toString(), metadata.entry));

	return Void();
}

ACTOR Future<Void> changeDataClusterConfig(ReadYourWritesTransaction* ryw,
                                           ClusterNameRef clusterName,
                                           std::vector<std::pair<StringRef, Optional<Value>>> configEntries) {
	state Optional<DataClusterMetadata> clusterMetadata =
	    wait(MetaclusterAPI::managementClusterTryGetCluster(&ryw->getTransaction(), clusterName));
	if (!clusterMetadata.present()) {
		TraceEvent(SevWarn, "ConfigureUnknownDataCluster").detail("ClusterName", clusterName);
		ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
		    false,
		    "set data cluster configuration",
		    format("cannot configure data cluster `%s': cluster not found", clusterName.toString().c_str())));
		throw special_keys_api_failure();
	}

	DataClusterMetadata& metadata = clusterMetadata.get();
	applyDataClusterConfig(ryw, clusterName, configEntries, &metadata);
	wait(MetaclusterAPI::managementClusterUpdateClusterMetadata(&ryw->getTransaction(), clusterName, metadata));

	return Void();
}

ACTOR Future<Void> removeClusterRange(ReadYourWritesTransaction* ryw,
                                      ClusterName beginCluster,
                                      ClusterName endCluster) {
	std::map<ClusterName, DataClusterMetadata> clusters = wait(MetaclusterAPI::managementClusterListClusters(
	    &ryw->getTransaction(), beginCluster, endCluster, CLIENT_KNOBS->TOO_MANY));

	if (clusters.size() == CLIENT_KNOBS->TOO_MANY) {
		TraceEvent(SevWarn, "RemoveClustersRangeTooLange")
		    .detail("BeginCluster", beginCluster)
		    .detail("EndCluster", endCluster);
		ryw->setSpecialKeySpaceErrorMsg(
		    ManagementAPIError::toJsonString(false, "remove cluster", "too many cluster to range remove"));
		throw special_keys_api_failure();
	}

	std::vector<Future<Void>> removeFutures;
	for (auto cluster : clusters) {
		removeFutures.push_back(MetaclusterAPI::managementClusterRemove(&ryw->getTransaction(), cluster.first));
	}

	wait(waitForAll(removeFutures));
	return Void();
}

Future<Void> MetaclusterInternalManagementClusterImpl::processDataClusterCommand(ReadYourWritesTransaction* ryw) {
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(
	    KeyRangeRef("management_cluster/data_cluster/"_sr, "management_cluster/data_cluster0"_sr)
	        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::METACLUSTER_INTERNAL).begin));

	std::vector<Future<Void>> clusterManagementFutures;

	std::vector<std::pair<KeyRangeRef, Optional<Value>>> mapMutations;
	std::map<ClusterNameRef, std::vector<std::pair<KeyRef, Optional<Value>>>> configMutations;

	for (auto itr : ranges) {
		if (!itr.value().first) {
			continue;
		}

		KeyRangeRef range =
		    itr.range()
		        .removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::METACLUSTER_INTERNAL).begin)
		        .removePrefix(MetaclusterInternalManagementClusterImpl::submoduleRange.begin)
		        .removePrefix("data_cluster/"_sr);

		KeyRef begin = range.begin;
		KeyRef end = range.end;
		KeyRef command = extractCommand(ryw, begin, end, itr.range(), "\xff\xff"_sr);

		if (command == "map"_sr) {
			mapMutations.push_back(std::make_pair(KeyRangeRef(begin, end), itr.value().second));
		} else if (command == "configure"_sr && KeyRangeRef(begin, end).singleKeyRange()) {
			// TODO: handle capacity/num_tenant_groups
			KeyRef param = begin.eat("/");
			configMutations[begin].push_back(std::make_pair(param, itr.value().second));
		}
	}

	for (auto mapMutation : mapMutations) {
		if (mapMutation.second.present()) {
			Optional<std::vector<std::pair<StringRef, Optional<Value>>>> registerMutations;
			auto itr = configMutations.find(mapMutation.first.begin);
			if (itr != configMutations.end()) {
				registerMutations = itr->second;
				configMutations.erase(itr);
			}
			clusterManagementFutures.push_back(registerDataCluster(
			    ryw, mapMutation.first.begin, mapMutation.second.get().toString(), registerMutations));
		} else {
			// For a single key clear, just issue the delete
			if (mapMutation.first.singleKeyRange()) {
				clusterManagementFutures.push_back(
				    MetaclusterAPI::managementClusterRemove(&ryw->getTransaction(), mapMutation.first.begin));
			} else {
				clusterManagementFutures.push_back(
				    removeClusterRange(ryw, mapMutation.first.begin, mapMutation.first.end));
			}
		}
	}

	for (auto configMutation : configMutations) {
		clusterManagementFutures.push_back(changeDataClusterConfig(ryw, configMutation.first, configMutation.second));
	}

	return waitForAll(clusterManagementFutures);
}

Future<Void> MetaclusterInternalManagementClusterImpl::processTenantCommand(ReadYourWritesTransaction* ryw) {
	// TODO
	return Void();
}

Future<Optional<std::string>> MetaclusterInternalManagementClusterImpl::commit(ReadYourWritesTransaction* ryw) {
	return tag(processDataClusterCommand(ryw) && processTenantCommand(ryw), Optional<std::string>());
}