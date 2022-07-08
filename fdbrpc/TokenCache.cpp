#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/TokenCache.h"
#include "fdbrpc/TokenSign.h"
#include "flow/network.h"

#include <boost/compute/detail/lru_cache.hpp>

struct TokenCacheImpl {
	struct CacheEntry {
		Arena arena;
		VectorRef<TenantNameRef> tenants;
		double expirationTime = 0.0;
	};

	boost::compute::detail::lru_cache<StringRef, CacheEntry> cache;
	TokenCacheImpl() : cache(FLOW_KNOBS->TOKEN_CACHE_SIZE) {}

	bool validate(TenantNameRef tenant, StringRef token);
	bool validateAndAdd(double currentTime, StringRef signature, StringRef token, NetworkAddress const& peer);
};

TokenCache::TokenCache() : impl(new TokenCacheImpl()) {}
TokenCache::~TokenCache() {
	delete impl;
}

void TokenCache::createInstance() {
	g_network->setGlobal(INetwork::enTokenCache, new TokenCache());
}

TokenCache& TokenCache::instance() {
	return *reinterpret_cast<TokenCache*>(g_network->global(INetwork::enTokenCache));
}

bool TokenCache::validate(TenantNameRef name, StringRef token) {
	return impl->validate(name, token);
}

bool TokenCacheImpl::validateAndAdd(double currentTime,
                                    StringRef signature,
                                    StringRef token,
                                    NetworkAddress const& peer) {
	Arena arena;
	authz::jwt::TokenRef t;
	if (!authz::jwt::parseToken(arena, t, token)) {
		TEST(true); // Token can't be parsed
		return false;
	}
	if (!t.keyId.present()) {
		TEST(true); // Token with no key id
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoKeyID");
		return false;
	}
	auto key = FlowTransport::transport().getPublicKeyByName(t.keyId.get());
	if (!key.present()) {
		TEST(true); // Token referencing non-existing key
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "UnknownKey");
		return false;
	} else if (!t.expiresAtUnixTime.present()) {
		TEST(true); // Token has no expiration time
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoExpirationTime");
		return false;
	} else if (double(t.expiresAtUnixTime.get()) <= currentTime) {
		TEST(true); // Expired token
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "Expired");
		return false;
	} else if (!t.notBeforeUnixTime.present()) {
		TEST(true); // Token has no not-before field
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoNotBefore");
		return false;
	} else if (double(t.notBeforeUnixTime.get()) > currentTime) {
		TEST(true); // Token has no not-before field
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "TokenNotYetValid");
		return false;
	} else if (!t.tenants.present()) {
		TEST(true); // Token with no tenants
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoTenants");
		return false;
	} else if (!authz::jwt::verifyToken(token, key.get())) {
		TEST(true); // Token with invalid signature
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "InvalidSignature");
		return false;
	} else {
		CacheEntry c;
		c.expirationTime = double(t.expiresAtUnixTime.get());
		for (auto tenant : t.tenants.get()) {
			c.tenants.push_back_deep(c.arena, tenant);
		}
		StringRef signature(c.arena, signature);
		cache.insert(signature, c);
		return true;
	}
}

bool TokenCacheImpl::validate(TenantNameRef name, StringRef token) {
	auto sig = authz::jwt::signaturePart(token);
	auto cachedEntry = cache.get(sig);
	double currentTime = g_network->timer();
	NetworkAddress peer = FlowTransport::transport().currentDeliveryPeerAddress();

	if (!cachedEntry.has_value()) {
		if (validateAndAdd(currentTime, sig, token, peer)) {
			cachedEntry = cache.get(sig);
		} else {
			return false;
		}
	}

	ASSERT(cachedEntry.has_value());

	auto& entry = cachedEntry.get();
	if (entry.expirationTime < currentTime) {
		TEST(true); // Read expired token from cache
		TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "Expired");
		return false;
	}
	bool tenantFound = false;
	for (auto const& t : entry.tenants) {
		if (t == name) {
			tenantFound = true;
			break;
		}
	}
	if (tenantFound) {
		TEST(true); // Valid token doesn't reference tenant
		TraceEvent(SevWarn, "TenantTokenMismatch").detail("From", peer).detail("Tenant", name.toString());
		return false;
	}
	return true;
}
