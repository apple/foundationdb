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

#if defined(NO_INTELLISENSE) && !defined(FDBRPC_TENANT_AUTH_ACTOR_G_H)
#define FDBRPC_TENANT_AUTH_ACTOR_G_H
#include "fdbrpc/TenantAuth.actor.g.h"
#elif !defined(FDBRPC_TENANT_AUTH_ACTOR_H)
#define FDBRPC_TENANT_AUTH_ACTOR_H

#include <string_view>
#include <queue>

#include "fdbrpc/TenantInfo.h"
#include "fdbrpc/TokenSign.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

class AuthorizedTenants : public ReferenceCounted<AuthorizedTenants> {
	friend class TransportData;
	using QueueMember = std::pair<double, TenantNameRef>;
	struct Cmp {
		bool operator()(QueueMember const& lhs, QueueMember const& rhs) const { return lhs.first > rhs.first; }
	};
	bool trusted;
	Future<Void> cleaner;
	ACTOR static Future<Void> clean(AuthorizedTenants* self) {
		loop {
			while (!self->queue.empty() && self->queue.top().first <= now()) {
				auto const& t = self->queue.top();
				self->authorizedTenants.erase(t.second);
				self->queue.pop();
			}
			Future<Void> nextExpire = self->queue.empty() ? Never() : delay(self->queue.top().first - now());
			choose {
				when(wait(nextExpire)) {}
				when(wait(self->insert.onTrigger())) {}
			}
		}
	}
	std::priority_queue<QueueMember, std::vector<QueueMember>, Cmp> queue;
	std::set<TenantName> authorizedTenants;
	AsyncTrigger insert;

public:
	AuthorizedTenants(bool trusted = false) : trusted(trusted) { cleaner = clean(this); }
	void add(double expire, VectorRef<TenantNameRef> const& tenants) {
		for (auto tenant : tenants) {
			TenantName t(tenant);
			queue.emplace(expire, t);
			authorizedTenants.insert(std::move(t));
		}
		insert.trigger();
	}
	bool contains(TenantNameRef tenant) const { return authorizedTenants.count(tenant) != 0; }
	bool isTrusted() const { return trusted; }
};

// TODO: receive and validate token instead
struct AuthorizationRequest {
	constexpr static FileIdentifier file_identifier = 11499331;

	Arena arena;
	VectorRef<SignedAuthTokenRef> tokens;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tokens, arena);
	}
};

template <>
struct serializable_traits<TenantInfo> : std::true_type {
	template <class Archiver>
	static void serialize(Archiver& ar, TenantInfo& v) {
		using namespace std::literals;
		serializer(ar, v.name, v.tenantId);
		if constexpr (Archiver::isDeserializing) {
			try {
				Reference<AuthorizedTenants>& authorizedTenants =
				    std::any_cast<Reference<AuthorizedTenants>&>(ar.context().variable("AuthorizedTenants"sv));
				v.trusted = authorizedTenants->isTrusted();
				v.verified = v.trusted || !v.name.present() || authorizedTenants->contains(v.name.get());
			} catch (std::out_of_range& e) {
				TraceEvent(SevError, "AttemptedReadTenantInfoWithNoAuth").backtrace();
				ASSERT(false);
			}
		}
	}
};

#endif
