/*
 * TenantSpecialKeys.actor.cpp
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
#include "fdbclient/Knobs.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<RangeResult> getTenantList(ReadYourWritesTransaction* ryw, KeyRangeRef kr, GetRangeLimits limitsHint) {
	state KeyRef managementPrefix =
	    kr.begin.substr(0,
	                    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin.size() +
	                        TenantMapRangeImpl::submoduleRange.begin.size());

	kr = kr.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
	TenantNameRef beginTenant = kr.begin.removePrefix(TenantMapRangeImpl::submoduleRange.begin);

	TenantNameRef endTenant = kr.end;
	if (endTenant.startsWith(TenantMapRangeImpl::submoduleRange.begin)) {
		endTenant = endTenant.removePrefix(TenantMapRangeImpl::submoduleRange.begin);
	} else {
		endTenant = "\xff"_sr;
	}

	std::map<TenantName, TenantMapEntry> tenants =
	    wait(TenantAPI::listTenantsTransaction(&ryw->getTransaction(), beginTenant, endTenant, limitsHint.rows));

	RangeResult results;
	for (auto tenant : tenants) {
		json_spirit::mObject tenantEntry;
		tenantEntry["id"] = tenant.second.id;
		tenantEntry["prefix"] = tenant.second.prefix.toString();
		std::string tenantEntryString = json_spirit::write_string(json_spirit::mValue(tenantEntry));
		ValueRef tenantEntryBytes(results.arena(), tenantEntryString);
		results.push_back(results.arena(),
		                  KeyValueRef(tenant.first.withPrefix(managementPrefix, results.arena()), tenantEntryBytes));
	}

	return results;
}

TenantMapRangeImpl::TenantMapRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> TenantMapRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                 KeyRangeRef kr,
                                                 GetRangeLimits limitsHint) const {
	return getTenantList(ryw, kr, limitsHint);
}

ACTOR Future<Void> createTenants(ReadYourWritesTransaction* ryw, std::vector<TenantNameRef> tenants) {
	Optional<Value> lastIdVal = wait(ryw->getTransaction().get(tenantLastIdKey));
	int64_t previousId = lastIdVal.present() ? TenantMapEntry::prefixToId(lastIdVal.get()) : -1;

	std::vector<Future<Void>> createFutures;
	for (auto tenant : tenants) {
		createFutures.push_back(
		    success(TenantAPI::createTenantTransaction(&ryw->getTransaction(), tenant, ++previousId)));
	}

	ryw->getTransaction().set(tenantLastIdKey, TenantMapEntry::idToPrefix(previousId));
	wait(waitForAll(createFutures));
	return Void();
}

ACTOR Future<Void> deleteTenantRange(ReadYourWritesTransaction* ryw,
                                     TenantNameRef beginTenant,
                                     TenantNameRef endTenant) {
	std::map<TenantName, TenantMapEntry> tenants =
	    wait(TenantAPI::listTenantsTransaction(&ryw->getTransaction(), beginTenant, endTenant, CLIENT_KNOBS->TOO_MANY));

	if (tenants.size() == CLIENT_KNOBS->TOO_MANY) {
		TraceEvent(SevWarn, "DeleteTenantRangeTooLange")
		    .detail("BeginTenant", beginTenant)
		    .detail("EndTenant", endTenant);
		ryw->setSpecialKeySpaceErrorMsg("too many tenants to range delete");
		throw special_keys_api_failure();
	}

	std::vector<Future<Void>> deleteFutures;
	for (auto tenant : tenants) {
		deleteFutures.push_back(TenantAPI::deleteTenantTransaction(&ryw->getTransaction(), tenant.first));
	}

	wait(waitForAll(deleteFutures));
	return Void();
}

Future<Optional<std::string>> TenantMapRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(range);
	std::vector<TenantNameRef> tenantsToCreate;
	std::vector<Future<Void>> tenantManagementFutures;
	for (auto range : ranges) {
		if (!range.value().first) {
			continue;
		}

		TenantNameRef tenantName =
		    range.begin()
		        .removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
		        .removePrefix(TenantMapRangeImpl::submoduleRange.begin);

		if (range.value().second.present()) {
			tenantsToCreate.push_back(tenantName);
		} else {
			// For a single key clear, just issue the delete
			if (KeyRangeRef(range.begin(), range.end()).singleKeyRange()) {
				tenantManagementFutures.push_back(
				    TenantAPI::deleteTenantTransaction(&ryw->getTransaction(), tenantName));
			} else {
				TenantNameRef endTenant = range.end().removePrefix(
				    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
				if (endTenant.startsWith(submoduleRange.begin)) {
					endTenant = endTenant.removePrefix(submoduleRange.begin);
				} else {
					endTenant = "\xff"_sr;
				}
				tenantManagementFutures.push_back(deleteTenantRange(ryw, tenantName, endTenant));
			}
		}
	}

	if (tenantsToCreate.size()) {
		tenantManagementFutures.push_back(createTenants(ryw, tenantsToCreate));
	}

	return tag(waitForAll(tenantManagementFutures), Optional<std::string>());
}