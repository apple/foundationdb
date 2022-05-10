/*
 * TenantInfo.h
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
#ifndef FDBRPC_TENANTINFO_H_
#define FDBRPC_TENANTINFO_H_
#include "flow/Arena.h"
#include "fdbrpc/fdbrpc.h"
#include <set>

struct TenantInfoRef {
	TenantInfoRef() {}
	TenantInfoRef(Arena& p, StringRef toCopy) : tenantName(StringRef(p, toCopy)) {}
	TenantInfoRef(Arena& p, TenantInfoRef toCopy)
	  : tenantName(toCopy.tenantName.present() ? Optional<StringRef>(StringRef(p, toCopy.tenantName.get()))
	                                           : Optional<StringRef>()) {}
	// Empty tenant name means that the peer is trusted
	Optional<StringRef> tenantName;

	bool operator<(TenantInfoRef const& other) const {
		if (!other.tenantName.present()) {
			return false;
		}
		if (!tenantName.present()) {
			return true;
		}
		return tenantName.get() < other.tenantName.get();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenantName);
	}
};

struct AuthorizedTenants : ReferenceCounted<AuthorizedTenants> {
	Arena arena;
	std::set<TenantInfoRef> authorizedTenants;
	bool trusted = false;
};

// TODO: receive and validate token instead
struct AuthorizationRequest {
	constexpr static FileIdentifier file_identifier = 11499331;

	Arena arena;
	VectorRef<TenantInfoRef> tenants;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenants, reply, arena);
	}
};

#endif // FDBRPC_TENANTINFO_H_
