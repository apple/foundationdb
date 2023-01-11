/*
 * EncryptionUtils.h
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

#ifndef FDBCLIENT_ENCRYPTION_UTIL_H
#define FDBCLIENT_ENCRYPTION_UTIL_H
#pragma once

#include "flow/EncryptUtils.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/SystemData.h"

inline EncryptCipherDomainId getEncryptDomainId(MutationRef m) {
	if (isSystemKey(m.param1)) {
		return SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID;
	} else if (m.type ==
	           MutationRef::SetVersionstampedKey) { // The first 8 bytes of the key of this OP is also an 8-byte number
		return FDB_DEFAULT_ENCRYPT_DOMAIN_ID;
	} else if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		ASSERT_NE((MutationRef::Type)m.type, MutationRef::Type::ClearRange);

		if (m.param1.size() >= TenantAPI::PREFIX_SIZE) {
			StringRef prefix = m.param1.substr(0, TenantAPI::PREFIX_SIZE);
			return TenantAPI::prefixToId(prefix, EnforceValidTenantId::False);
		}
		return INVALID_ENCRYPT_DOMAIN_ID;
	} else {
		// ClearRange is the 'only' MultiKey transaction allowed
		ASSERT_EQ((MutationRef::Type)m.type, MutationRef::Type::ClearRange);

		// FIXME: Handle Clear-range transaction, actions needed:
		// 1. Transaction range can spawn multiple encryption domains (tenants)
		// 2. Transaction can be a multi-key transaction spawning multiple tenants
		// For now fallback to 'default encryption domain'

		CODE_PROBE(true, "ClearRange mutation encryption");
		return FDB_DEFAULT_ENCRYPT_DOMAIN_ID;
	}
}

#endif // FDBCLIENT_ENCRYPTION_UTIL_H
