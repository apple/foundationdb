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

template <bool HasSubRanges = true>
class TenantRangeImpl : public SpecialKeyRangeRWImpl {
private:
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
		              TenantRangeImpl::submoduleRange.begin.size() + TenantRangeImpl::mapSubRange.begin.size() +
		              key.size();

		KeyRef prefixedKey = makeString(keySize, ar);
		uint8_t* mutableKey = mutateString(prefixedKey);

		mutableKey = SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin.copyTo(mutableKey);
		mutableKey = TenantRangeImpl::submoduleRange.begin.copyTo(mutableKey);
		mutableKey = TenantRangeImpl::mapSubRange.begin.copyTo(mutableKey);

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
			std::string tenantEntryString = json_spirit::write_string(json_spirit::mValue(tenantEntry));
			ValueRef tenantEntryBytes(results->arena(), tenantEntryString);
			results->push_back(results->arena(),
			                   KeyValueRef(withTenantMapPrefix(tenant.first, results->arena()), tenantEntryBytes));
		}

		return Void();
	}

	ACTOR static Future<RangeResult> getTenantRange(ReadYourWritesTransaction* ryw,
	                                                KeyRangeRef kr,
	                                                GetRangeLimits limitsHint) {
		state RangeResult results;

		kr = kr.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
		         .removePrefix(TenantRangeImpl::submoduleRange.begin);

		if (kr.intersects(TenantRangeImpl::mapSubRange)) {
			GetRangeLimits limits = limitsHint;
			limits.decrement(results);
			wait(getTenantList(
			    ryw,
			    removePrefix(kr & TenantRangeImpl::mapSubRange, TenantRangeImpl::mapSubRange.begin, "\xff"_sr),
			    &results,
			    limits));
		}

		return results;
	}

	ACTOR static Future<Void> createTenants(ReadYourWritesTransaction* ryw, std::vector<TenantNameRef> tenants) {
		int64_t _nextId = wait(TenantAPI::getNextTenantId(&ryw->getTransaction()));
		int64_t nextId = _nextId;

		std::vector<Future<Void>> createFutures;
		for (auto tenant : tenants) {
			createFutures.push_back(
			    success(TenantAPI::createTenantTransaction(&ryw->getTransaction(), tenant, nextId++)));
		}

		ryw->getTransaction().set(tenantLastIdKey, TenantMapEntry::idToPrefix(nextId - 1));
		wait(waitForAll(createFutures));
		return Void();
	}

	ACTOR static Future<Void> deleteTenantRange(ReadYourWritesTransaction* ryw,
	                                            TenantName beginTenant,
	                                            TenantName endTenant) {
		state std::map<TenantName, TenantMapEntry> tenants = wait(
		    TenantAPI::listTenantsTransaction(&ryw->getTransaction(), beginTenant, endTenant, CLIENT_KNOBS->TOO_MANY));

		if (tenants.size() == CLIENT_KNOBS->TOO_MANY) {
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
	const static KeyRangeRef submoduleRange;
	const static KeyRangeRef mapSubRange;

	explicit TenantRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override {
		return getTenantRange(ryw, kr, limitsHint);
	}

	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override {
		auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(range);
		std::vector<Future<Void>> tenantManagementFutures;

		std::vector<std::pair<KeyRangeRef, Optional<Value>>> mapMutations;

		for (auto range : ranges) {
			if (!range.value().first) {
				continue;
			}

			KeyRangeRef adjustedRange =
			    range.range()
			        .removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
			        .removePrefix(submoduleRange.begin);

			if (mapSubRange.intersects(adjustedRange)) {
				adjustedRange = mapSubRange & adjustedRange;
				adjustedRange = removePrefix(adjustedRange, mapSubRange.begin, "\xff"_sr);
				mapMutations.push_back(std::make_pair(adjustedRange, range.value().second));
			}
		}

		std::vector<TenantNameRef> tenantsToCreate;
		for (auto mapMutation : mapMutations) {
			TenantNameRef tenantName = mapMutation.first.begin;
			if (mapMutation.second.present()) {
				tenantsToCreate.push_back(tenantName);
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

		return tag(waitForAll(tenantManagementFutures), Optional<std::string>());
	}
};

#include "flow/unactorcompiler.h"
#endif