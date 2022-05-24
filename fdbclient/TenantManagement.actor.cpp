/*
 * TenantManagement.actor.cpp
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

#include <string>
#include <map>
#include "fdbclient/SystemData.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Tuple.h"
#include "flow/actorcompiler.h" // has to be last include

namespace ManagementAPI {

bool checkTenantMode(Optional<Value> tenantModeValue, bool isDataCluster, TenantOperationType operationType) {
	TenantMode tenantMode = TenantMode::fromValue(tenantModeValue.castTo<ValueRef>());

	if (tenantMode == TenantMode::DISABLED) {
		return false;
	} else if (operationType == TenantOperationType::MANAGEMENT_CLUSTER && tenantMode != TenantMode::MANAGEMENT) {
		return false;
	} else if (operationType == TenantOperationType::DATA_CLUSTER &&
	           (tenantMode != TenantMode::REQUIRED || !isDataCluster)) {
		return false;
	} else if (operationType == TenantOperationType::STANDALONE_CLUSTER &&
	           (tenantMode == TenantMode::MANAGEMENT || isDataCluster)) {
		return false;
	}

	return true;
}

Key getTenantGroupIndexKey(TenantGroupNameRef tenantGroup, Optional<TenantNameRef> tenant) {
	Tuple tuple;
	tuple.append(tenantGroup);
	if (tenant.present()) {
		tuple.append(tenant.get());
	}
	return tenantGroupTenantIndexKeys.begin.withSuffix(tuple.pack());
}

} // namespace ManagementAPI
