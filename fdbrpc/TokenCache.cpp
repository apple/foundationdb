#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/TokenCache.h"
#include "fdbrpc/TokenSign.h"
#include "flow/network.h"

#include <boost/compute/detail/lru_cache.hpp>
#include <boost/unordered_set.hpp>

struct TokenCacheImpl {
	struct CacheEntry {
		Arena arena;
		boost::unordered_set<TenantNameRef> tenants;
		double expirationTime = 0.0;
	};

	boost::compute::detail::lru_cache<StringRef, CacheEntry> cache;
	TokenCacheImpl() : cache(FLOW_KNOBS->TOKEN_CACHE_SIZE) {}

	bool validate(TenantNameRef tenant, StringRef token);
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

bool TokenCacheImpl::validate(TenantNameRef name, StringRef token) {
	auto sig = authz::jwt::signaturePart(token);
	auto cachedEntry = cache.get(sig);
	double currentTime = g_network->timer();
	NetworkAddress peer = FlowTransport::transport().currentDeliveryPeerAddress();

	if (cachedEntry.has_value()) {
		auto& entry = cachedEntry.get();
		if (entry.expirationTime > currentTime) {
			TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "Expired");
			throw permission_denied();
		}
		if (entry.tenants.count(name) == 0) {
		    TraceEvent(SevWarn, "TenantTokenMismatch").detail("From", peer).detail("Tenant", name.toString());
		    throw permission_denied();
		}
		return true;
	} else {
		Arena arena;
		authz::jwt::TokenRef t;
		if (!authz::jwt::parseToken(arena, t, token)) {
			throw permission_denied();
		}
		if (!t.keyId.present()) {
			TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoKeyID");
			return false;
		}
		auto key = FlowTransport::transport().getPublicKeyByName(t.keyId.get());
		if (key.present() && authz::jwt::verifyToken(token, key.get())) {
			if (!t.expiresAtUnixTime.present()) {
				TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoExpirationTime");
				throw permission_denied();
			} else if (double(t.expiresAtUnixTime.get()) <= currentTime) {
				TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "Expired");
				return false;
			}
			if (!t.notBeforeUnixTime.present()) {
				TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoNotBefore");
				return false;
			} else if (double(t.notBeforeUnixTime.get()) > currentTime) {
				TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "TokenNotYetValid");
				return false;
			}
			if (!t.tenants.present()) {
				TraceEvent(SevWarn, "InvalidToken").detail("From", peer).detail("Reason", "NoTenants");
				return false;
			}
			CacheEntry c;
			c.expirationTime = double(t.expiresAtUnixTime.get());
			for (auto tenant : t.tenants.get()) {
				c.tenants.insert(StringRef(c.arena, tenant));
			}
			StringRef signature(c.arena, sig);
			return true;
		} else {
			TraceEvent(SevWarn, "InvalidSignature")
			    .detail("From", peer)
			    .detail("Reason", key.present() ? "VerificationFailed" : "KeyNotFound");
			return false;
		}
	}
}
