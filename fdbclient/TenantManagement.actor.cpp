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
#include "fdbclient/Atomic.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/Tuple.h"
#include "flow/actorcompiler.h" // has to be last include

namespace TenantAPI {

TenantMode tenantModeForClusterType(ClusterType clusterType, TenantMode tenantMode) {
	if (clusterType == ClusterType::METACLUSTER_MANAGEMENT) {
		return TenantMode::DISABLED;
	} else if (clusterType == ClusterType::METACLUSTER_DATA) {
		return TenantMode::REQUIRED;
	} else {
		return tenantMode;
	}
}

int64_t extractTenantIdFromMutation(MutationRef m) {
	ASSERT(!isSystemKey(m.param1));

	if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		// The first 8 bytes of the key of this OP is also an 8-byte number
		if (m.type == MutationRef::SetVersionstampedKey && m.param1.size() >= 4 &&
		    parseVersionstampOffset(m.param1) < 8) {
			return TenantInfo::INVALID_TENANT;
		}
	} else {
		// Assumes clear range mutations are split on tenant boundaries
		ASSERT_EQ(m.type, MutationRef::Type::ClearRange);
	}

	return extractTenantIdFromKeyRef(m.param1);
}

int64_t extractTenantIdFromKeyRef(StringRef s) {
	if (s.size() < TenantAPI::PREFIX_SIZE) {
		return TenantInfo::INVALID_TENANT;
	}
	// Parse mutation key to determine tenant prefix
	StringRef prefix = s.substr(0, TenantAPI::PREFIX_SIZE);
	return TenantAPI::prefixToId(prefix, EnforceValidTenantId::False);
}

} // namespace TenantAPI
