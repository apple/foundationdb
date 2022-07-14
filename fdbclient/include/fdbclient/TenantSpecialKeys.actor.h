/*
 * TenantSpecialKeys.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_G_H)
#define FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_G_H
#include "fdbclient/TenantSpecialKeys.actor.g.h"
#elif !defined(FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_H)
#define FDBCLIENT_TENANT_SPECIAL_KEYS_ACTOR_H

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/libb64/encode.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

template <bool HasSubRanges>
class TenantRangeImpl : public SpecialKeyRangeRWImpl {
private:
	static bool subRangeIntersects(KeyRangeRef subRange, KeyRangeRef range);

	static KeyRangeRef removePrefix(KeyRangeRef range, KeyRef prefix, KeyRef defaultEnd) {
		KeyRef begin = range.begin.removePrefix(prefix);
		KeyRef end;
		if (range.end.startsWith(prefix)) {
			end = range.end.removePrefix(prefix);
		} else {
			end = defaultEnd;
		}

		return KeyRangeRef(begin, end);
	}

	static KeyRef withTenantMapPrefix(KeyRef key, Arena& ar) {
		int keySize = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin.size() +
		              submoduleRange.begin.size() + mapSubRange.begin.size() + key.size();

		KeyRef prefixedKey = makeString(keySize, ar);
		uint8_t* mutableKey = mutateString(prefixedKey);

		mutableKey = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin.copyTo(mutableKey);
		mutableKey = submoduleRange.begin.copyTo(mutableKey);
		mutableKey = mapSubRange.begin.copyTo(mutableKey);

		key.copyTo(mutableKey);
		return prefixedKey;
	}

	ACTOR static Future<Void> getTenantList(ReadYourWritesTransaction* ryw,
	                                        KeyRangeRef kr,
	                                        RangeResult* results,
	                                        GetRangeLimits limitsHint) {
		std::map<TenantName, TenantMapEntry> tenants =
		    wait(TenantAPI::listTenantsTransaction(&ryw->getTransaction(), kr.begin, kr.end, limitsHint.rows));

		for (auto tenant : tenants) {
			json_spirit::mObject tenantEntry;
			tenantEntry["id"] = tenant.second.id;

			if (ryw->getDatabase()->apiVersionAtLeast(720)) {
				json_spirit::mObject prefixObject;
				std::string encodedPrefix = base64::encoder::from_string(tenant.second.prefix.toString());
				// Remove trailing newline
				encodedPrefix.resize(encodedPrefix.size() - 1);

				prefixObject["base64"] = encodedPrefix;
				prefixObject["printable"] = printable(tenant.second.prefix);
				tenantEntry["prefix"] = prefixObject;
			} else {
				// This is not a standard encoding in JSON, and some libraries may not be able to easily decode it
				tenantEntry["prefix"] = tenant.second.prefix.toString();
			}

			tenantEntry["tenant_state"] = TenantMapEntry::tenantStateToString(tenant.second.tenantState);
			if (tenant.second.assignedCluster.present()) {
				tenantEntry["assigned_cluster"] = tenant.second.assignedCluster.get().toString();
			}
			if (tenant.second.tenantGroup.present()) {
				tenantEntry["tenant_group"] = tenant.second.tenantGroup.get().toString();
			}
			std::string tenantEntryString = json_spirit::write_string(json_spirit::mValue(tenantEntry));
			ValueRef tenantEntryBytes(results->arena(), tenantEntryString);
			results->push_back(results->arena(),
			                   KeyValueRef(withTenantMapPrefix(tenant.first, results->arena()), tenantEntryBytes));
		}

		return Void();
	}

	ACTOR template <bool B>
	static Future<RangeResult> getTenantRange(ReadYourWritesTransaction* ryw,
	                                          KeyRangeRef kr,
	                                          GetRangeLimits limitsHint) {
		state RangeResult results;

		kr = kr.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
		         .removePrefix(TenantRangeImpl<B>::submoduleRange.begin);

		if (kr.intersects(TenantRangeImpl<B>::mapSubRange)) {
			GetRangeLimits limits = limitsHint;
			limits.decrement(results);
			wait(getTenantList(
			    ryw,
			    removePrefix(kr & TenantRangeImpl<B>::mapSubRange, TenantRangeImpl<B>::mapSubRange.begin, "\xff"_sr),
			    &results,
			    limits));
		}

		return results;
	}

	ACTOR static Future<bool> checkTenantGroup(ReadYourWritesTransaction* ryw,
	                                           Optional<TenantGroupName> currentGroup,
	                                           Optional<TenantGroupName> desiredGroup) {
		if (!desiredGroup.present() || currentGroup == desiredGroup) {
			return true;
		}

		// TODO: check where desired group is assigned and allow if the cluster is the same
		// SOMEDAY: It should also be possible to change the tenant group when we support tenant movement.
		wait(delay(0));

		return false;
	}

	ACTOR static Future<Void> applyTenantConfig(ReadYourWritesTransaction* ryw,
	                                            TenantNameRef tenantName,
	                                            std::vector<std::pair<StringRef, Optional<Value>>> configEntries,
	                                            TenantMapEntry* tenantEntry,
	                                            bool creatingTenant) {

		state std::vector<std::pair<StringRef, Optional<Value>>>::iterator configItr;
		for (configItr = configEntries.begin(); configItr != configEntries.end(); ++configItr) {
			if (configItr->first == "tenant_group"_sr) {
				state bool isValidTenantGroup = true;
				if (!creatingTenant) {
					bool result = wait(checkTenantGroup(ryw, tenantEntry->tenantGroup, configItr->second));
					isValidTenantGroup = result;
				}
				if (isValidTenantGroup) {
					tenantEntry->tenantGroup = configItr->second;
				} else {
					TraceEvent(SevWarn, "CannotChangeTenantGroup")
					    .detail("TenantName", tenantName)
					    .detail("CurrentTenantGroup", tenantEntry->tenantGroup)
					    .detail("DesiredTenantGroup", configItr->second);
					ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
					    false,
					    "set tenant configuration",
					    format("cannot change tenant group for tenant `%s'", tenantName.toString().c_str())));
					throw special_keys_api_failure();
				}
			} else {
				TraceEvent(SevWarn, "InvalidTenantConfig")
				    .detail("TenantName", tenantName)
				    .detail("ConfigName", configItr->first);
				ryw->setSpecialKeySpaceErrorMsg(
				    ManagementAPIError::toJsonString(false,
				                                     "set tenant configuration",
				                                     format("invalid tenant configuration option `%s' for tenant `%s'",
				                                            configItr->first.toString().c_str(),
				                                            tenantName.toString().c_str())));
				throw special_keys_api_failure();
			}
		}

		return Void();
	}

	ACTOR static Future<Void> createTenant(ReadYourWritesTransaction* ryw,
	                                       TenantNameRef tenantName,
	                                       Optional<std::vector<std::pair<StringRef, Optional<Value>>>> configMutations,
	                                       int64_t tenantId) {
		state TenantMapEntry tenantEntry;
		tenantEntry.id = tenantId;

		if (configMutations.present()) {
			wait(applyTenantConfig(ryw, tenantName, configMutations.get(), &tenantEntry, true));
		}

		std::pair<Optional<TenantMapEntry>, bool> entry =
		    wait(TenantAPI::createTenantTransaction(&ryw->getTransaction(), tenantName, tenantEntry));

		return Void();
	}

	ACTOR static Future<Void> createTenants(
	    ReadYourWritesTransaction* ryw,
	    std::map<TenantNameRef, Optional<std::vector<std::pair<StringRef, Optional<Value>>>>> tenants) {
		int64_t _nextId = wait(TenantAPI::getNextTenantId(&ryw->getTransaction()));
		int64_t nextId = _nextId;

		std::vector<Future<Void>> createFutures;
		for (auto const& [tenant, config] : tenants) {
			createFutures.push_back(createTenant(ryw, tenant, config, nextId++));
		}

		ryw->getTransaction().set(tenantLastIdKey, TenantMapEntry::idToPrefix(nextId - 1));
		wait(waitForAll(createFutures));
		return Void();
	}

	ACTOR static Future<Void> changeTenantConfig(ReadYourWritesTransaction* ryw,
	                                             TenantNameRef tenantName,
	                                             std::vector<std::pair<StringRef, Optional<Value>>> configEntries) {
		state Optional<TenantMapEntry> tenantEntry = wait(TenantAPI::tryGetTenantTransaction(ryw, tenantName));
		if (!tenantEntry.present()) {
			TraceEvent(SevWarn, "ConfigureUnknownTenant").detail("TenantName", tenantName);
			ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
			    false,
			    "set tenant configuration",
			    format("cannot configure tenant `%s': tenant not found", tenantName.toString().c_str())));
			throw special_keys_api_failure();
		}

		TenantMapEntry& entry = tenantEntry.get();
		wait(applyTenantConfig(ryw, tenantName, configEntries, &entry, false));
		TenantAPI::configureTenantTransaction(ryw, tenantName, tenantEntry.get());

		return Void();
	}

	ACTOR static Future<Void> deleteTenantRange(ReadYourWritesTransaction* ryw,
	                                            TenantName beginTenant,
	                                            TenantName endTenant) {
		state Future<Optional<Value>> tenantModeFuture =
		    ryw->getTransaction().get(configKeysPrefix.withSuffix("tenant_mode"_sr));
		state Future<ClusterType> clusterTypeFuture = TenantAPI::getClusterType(&ryw->getTransaction());

		state std::map<TenantName, TenantMapEntry> tenants = wait(TenantAPI::listTenantsTransaction(
		    &ryw->getTransaction(), beginTenant, endTenant, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));

		state Optional<Value> tenantMode = wait(safeThreadFutureToFuture(tenantModeFuture));
		ClusterType clusterType = wait(clusterTypeFuture);

		if (!TenantAPI::checkTenantMode(tenantMode, clusterType, ClusterType::STANDALONE)) {
			throw tenants_disabled();
		}

		if (tenants.size() > CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER) {
			TraceEvent(SevWarn, "DeleteTenantRangeTooLange")
			    .detail("BeginTenant", beginTenant)
			    .detail("EndTenant", endTenant);
			ryw->setSpecialKeySpaceErrorMsg(
			    ManagementAPIError::toJsonString(false, "delete tenants", "too many tenants to range delete"));
			throw special_keys_api_failure();
		}

		std::vector<Future<Void>> deleteFutures;
		for (auto tenant : tenants) {
			deleteFutures.push_back(TenantAPI::deleteTenantTransaction(&ryw->getTransaction(), tenant.first));
		}

		wait(waitForAll(deleteFutures));
		return Void();
	}

public:
	// These ranges vary based on the template parameter
	const static KeyRangeRef submoduleRange;
	const static KeyRangeRef mapSubRange;

	// These sub-ranges should only be used if HasSubRanges=true
	const inline static KeyRangeRef configureSubRange = KeyRangeRef("configure/"_sr, "configure0"_sr);

	explicit TenantRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override {
		return getTenantRange<HasSubRanges>(ryw, kr, limitsHint);
	}

	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override {
		auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(range);
		std::vector<Future<Void>> tenantManagementFutures;

		std::vector<std::pair<KeyRangeRef, Optional<Value>>> mapMutations;
		std::map<TenantNameRef, std::vector<std::pair<StringRef, Optional<Value>>>> configMutations;

		for (auto range : ranges) {
			if (!range.value().first) {
				continue;
			}

			KeyRangeRef adjustedRange =
			    range.range()
			        .removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
			        .removePrefix(submoduleRange.begin);

			if (subRangeIntersects(mapSubRange, adjustedRange)) {
				adjustedRange = mapSubRange & adjustedRange;
				adjustedRange = removePrefix(adjustedRange, mapSubRange.begin, "\xff"_sr);
				mapMutations.push_back(std::make_pair(adjustedRange, range.value().second));
			} else if (subRangeIntersects(configureSubRange, adjustedRange) && adjustedRange.singleKeyRange()) {
				StringRef tenantName = adjustedRange.begin.removePrefix(configureSubRange.begin);
				StringRef configName = tenantName.eat("/");
				configMutations[tenantName].push_back(std::make_pair(configName, range.value().second));
			}
		}

		std::map<TenantNameRef, Optional<std::vector<std::pair<StringRef, Optional<Value>>>>> tenantsToCreate;
		for (auto mapMutation : mapMutations) {
			TenantNameRef tenantName = mapMutation.first.begin;
			if (mapMutation.second.present()) {
				Optional<std::vector<std::pair<StringRef, Optional<Value>>>> createMutations;
				auto itr = configMutations.find(tenantName);
				if (itr != configMutations.end()) {
					createMutations = itr->second;
					configMutations.erase(itr);
				}
				tenantsToCreate[tenantName] = createMutations;
			} else {
				// For a single key clear, just issue the delete
				if (mapMutation.first.singleKeyRange()) {
					tenantManagementFutures.push_back(
					    TenantAPI::deleteTenantTransaction(&ryw->getTransaction(), tenantName));
				} else {
					tenantManagementFutures.push_back(deleteTenantRange(ryw, tenantName, mapMutation.first.end));
				}
			}
		}

		if (!tenantsToCreate.empty()) {
			tenantManagementFutures.push_back(createTenants(ryw, tenantsToCreate));
		}
		for (auto configMutation : configMutations) {
			tenantManagementFutures.push_back(changeTenantConfig(ryw, configMutation.first, configMutation.second));
		}

		return tag(waitForAll(tenantManagementFutures), Optional<std::string>());
	}
};

#include "flow/unactorcompiler.h"
#endif