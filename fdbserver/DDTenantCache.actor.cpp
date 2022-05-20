/*
 * DDTenantCache.actor.cpp
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

#include "fdbserver/DDTenantCache.h"

class DDTenantCacheImpl {

	ACTOR static Future<RangeResult> getTenantList(DDTenantCache* self, Transaction* tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

		state Future<RangeResult> tenantList = tr->getRange(tenantMapKeys, CLIENT_KNOBS->TOO_MANY);
		wait(success(tenantList));
		ASSERT(!tenantList.get().more && tenantList.get().size() < CLIENT_KNOBS->TOO_MANY);

		return tenantList.get();
	}

public:
	ACTOR static Future<Void> build(DDTenantCache* self) {
		state Transaction tr(self->dbcx());

		TraceEvent(SevInfo, "BuildingDDTenantCache", self->id()).log();

		try {
			state RangeResult tenantList = wait(getTenantList(self, &tr));

			for (int i = 0; i < tenantList.size(); i++) {
				TenantName tname = tenantList[i].key.removePrefix(tenantMapPrefix);
				TenantMapEntry t = decodeTenantEntry(tenantList[i].value);

				TenantInfo tinfo(tname, t.id);
				self->tenantCache[t.id] = makeReference<TCTenantInfo>(tinfo);

				TraceEvent(SevInfo, "DDTenantFound", self->id()).detail("TenantName", tname).detail("TenantID", t.id);
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}

		TraceEvent(SevInfo, "BuiltDDTenantCache", self->id()).log();

		return Void();
	}

	ACTOR static Future<Void> monitorTenantMap(DDTenantCache* self) {
		TraceEvent(SevInfo, "StartingTenantCacheMonitor", self->id()).log();

		state Transaction tr(self->dbcx());

		state double lastTenantListFetchTime = now();

		loop {
			try {
				if (now() - lastTenantListFetchTime > (2 * SERVER_KNOBS->DD_TENANT_LIST_REFRESH_INTERVAL)) {
					TraceEvent(SevWarn, "DDTenantListRefreshDelay", self->id()).log();
				}

				state RangeResult tenantList = wait(getTenantList(self, &tr));

				DDTenantMap updatedTenantCache;
				bool tenantListUpdated = false;

				for (int i = 0; i < tenantList.size(); i++) {
					TenantName tname = tenantList[i].key.removePrefix(tenantMapPrefix);
					TenantMapEntry t = decodeTenantEntry(tenantList[i].value);

					TenantInfo tinfo(tname, t.id);
					updatedTenantCache[t.id] = makeReference<TCTenantInfo>(tinfo);

					if (!self->tenantCache.count(t.id)) {
						tenantListUpdated = true;
					}
				}

				if (self->tenantCache.size() != updatedTenantCache.size()) {
					tenantListUpdated = true;
				}

				if (tenantListUpdated) {
					self->tenantCache.swap(updatedTenantCache);
					TraceEvent(SevInfo, "DDTenantCache", self->id()).detail("List", self->desc());
				}

				updatedTenantCache.clear();
				tr.reset();
				lastTenantListFetchTime = now();
				wait(delay(2.0));
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("DDTenantCacheGetTenantListError", self->id()).errorUnsuppressed(e).suppressFor(1.0);
				}
				wait(tr.onError(e));
			}
		}
	}
};

Future<Void> DDTenantCache::build(Database cx) {
	return DDTenantCacheImpl::build(this);
}

Future<Void> DDTenantCache::monitorTenantMap() {
	return DDTenantCacheImpl::monitorTenantMap(this);
}