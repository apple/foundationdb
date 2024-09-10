#include "fdbrpc/Base64Encode.h"
#include "fdbrpc/Base64Decode.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/TokenCache.h"
#include "fdbrpc/TokenSign.h"
#include "fdbrpc/TenantInfo.h"
#include "flow/MkCert.h"
#include "flow/ScopeExit.h"
#include "flow/UnitTest.h"
#include "flow/network.h"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <list>
#include <deque>

#include "flow/actorcompiler.h" // has to be last include

using authz::TenantId;

template <class Key, class Value>
class LRUCache {
public:
	using key_type = Key;
	using list_type = std::list<key_type>;
	using mapped_type = Value;
	using map_type = boost::unordered_map<key_type, std::pair<mapped_type, typename list_type::iterator>>;
	using size_type = unsigned;

	explicit LRUCache(size_type capacity) : _capacity(capacity) { _map.reserve(capacity); }

	size_type size() const { return _map.size(); }
	size_type capacity() const { return _capacity; }
	bool empty() const { return _map.empty(); }

	Optional<mapped_type*> get(key_type const& key) {
		auto i = _map.find(key);
		if (i == _map.end()) {
			return Optional<mapped_type*>();
		}
		auto j = i->second.second;
		if (j != _list.begin()) {
			_list.erase(j);
			_list.push_front(i->first);
			i->second.second = _list.begin();
		}
		return &i->second.first;
	}

	template <class K, class V>
	mapped_type* insert(K&& key, V&& value) {
		auto iter = _map.find(key);
		if (iter != _map.end()) {
			return &iter->second.first;
		}
		if (size() == capacity()) {
			auto i = --_list.end();
			_map.erase(*i);
			_list.erase(i);
		}
		_list.push_front(std::forward<K>(key));
		std::tie(iter, std::ignore) =
		    _map.insert(std::make_pair(*_list.begin(), std::make_pair(std::forward<V>(value), _list.begin())));
		return &iter->second.first;
	}

private:
	const size_type _capacity;
	map_type _map;
	list_type _list;
};

TEST_CASE("/fdbrpc/authz/LRUCache") {
	auto& rng = *deterministicRandom();
	{
		// test very small LRU cache
		LRUCache<int, StringRef> cache(rng.randomInt(2, 10));
		for (int i = 0; i < 200; ++i) {
			cache.insert(i, "val"_sr);
			if (i >= cache.capacity()) {
				for (auto j = 0; j <= i - cache.capacity(); j++)
					ASSERT(!cache.get(j).present());
				// ordering is important so as not to disrupt the LRU order
				for (auto j = i - cache.capacity() + 1; j <= i; j++)
					ASSERT(cache.get(j).present());
			}
		}
	}
	{
		// Test larger cache
		LRUCache<int, StringRef> cache(1000);
		for (auto i = 0; i < 1000; ++i) {
			cache.insert(i, "value"_sr);
		}
		cache.insert(1000, "value"_sr); // should evict 0
		ASSERT(!cache.get(0).present());
	}
	{
		// memory test -- this is what the boost implementation didn't do correctly
		LRUCache<StringRef, Standalone<StringRef>> cache(10);
		std::deque<std::string> cachedStrings;
		std::deque<std::string> evictedStrings;
		for (int i = 0; i < 10; ++i) {
			auto str = rng.randomAlphaNumeric(rng.randomInt(100, 1024));
			Standalone<StringRef> sref(str);
			cache.insert(sref, sref);
			cachedStrings.push_back(str);
		}
		for (int i = 0; i < 10; ++i) {
			Standalone<StringRef> existingStr(cachedStrings.back());
			auto cachedStr = cache.get(existingStr);
			ASSERT(cachedStr.present());
			ASSERT(*cachedStr.get() == existingStr);
			if (!evictedStrings.empty()) {
				Standalone<StringRef> nonexisting(evictedStrings.at(rng.randomInt(0, evictedStrings.size())));
				ASSERT(!cache.get(nonexisting).present());
			}
			auto str = rng.randomAlphaNumeric(rng.randomInt(100, 1024));
			Standalone<StringRef> sref(str);
			evictedStrings.push_back(cachedStrings.front());
			cachedStrings.pop_front();
			cachedStrings.push_back(str);
			cache.insert(sref, sref);
		}
	}
	return Void();
}

struct CacheEntry {
	Arena arena;
	VectorRef<TenantId> tenants;
	Optional<StringRef> tokenId;
	double expirationTime = 0.0;
};

struct AuditEntry {
	NetworkAddress address;
	TenantId tenantId;
	Optional<Standalone<StringRef>> tokenId;
	bool operator==(const AuditEntry& other) const noexcept = default;
	explicit AuditEntry(NetworkAddress const& address, TenantId tenantId, CacheEntry const& cacheEntry)
	  : address(address), tenantId(tenantId),
	    tokenId(cacheEntry.tokenId.present() ? Standalone<StringRef>(cacheEntry.tokenId.get(), cacheEntry.arena)
	                                         : Optional<Standalone<StringRef>>()) {}
};

std::size_t hash_value(AuditEntry const& value) {
	std::size_t seed = 0;
	boost::hash_combine(seed, value.address);
	boost::hash_combine(seed, value.tenantId);
	if (value.tokenId.present()) {
		boost::hash_combine(seed, value.tokenId.get());
	}
	return seed;
}

struct TokenCacheImpl {
	TokenCacheImpl();
	LRUCache<StringRef, CacheEntry> cache;
	boost::unordered_set<AuditEntry> usedTokens;
	double lastResetTime;

	bool validate(TenantId tenantId, StringRef token);
	bool validateAndAdd(double currentTime, StringRef token, NetworkAddress const& peer);
	void logTokenUsage(double currentTime, AuditEntry&& entry);
};

TokenCacheImpl::TokenCacheImpl() : cache(FLOW_KNOBS->TOKEN_CACHE_SIZE), usedTokens(), lastResetTime(0) {}

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

bool TokenCache::validate(TenantId tenantId, StringRef token) {
	return impl->validate(tenantId, token);
}

#define TRACE_INVALID_PARSED_TOKEN(reason, token)                                                                      \
	TraceEvent(SevWarn, "InvalidToken"_audit)                                                                          \
	    .detail("From", peer)                                                                                          \
	    .detail("Reason", reason)                                                                                      \
	    .detail("CurrentTime", currentTime)                                                                            \
	    .detail("Token", toStringRef(arena, token).toStringView())

bool TokenCacheImpl::validateAndAdd(double currentTime, StringRef token, NetworkAddress const& peer) {
	Arena arena;
	authz::jwt::TokenRef t;
	StringRef signInput;
	Optional<StringRef> err;
	bool verifyOutcome;
	if ((err = authz::jwt::parseToken(arena, token, t, signInput)).present()) {
		CODE_PROBE(true, "Token can't be parsed");
		TraceEvent te(SevWarn, "InvalidToken");
		te.detail("From", peer);
		te.detail("Reason", "ParseError");
		te.detail("ErrorDetail", err.get());
		if (signInput.empty()) { // unrecognizable token structure
			te.detail("Token", token.toString());
		} else { // trace with signature part taken out
			te.detail("SignInput", signInput.toString());
		}
		return false;
	}
	auto key = FlowTransport::transport().getPublicKeyByName(t.keyId);
	if (!key.present()) {
		CODE_PROBE(true, "Token referencing non-existing key");
		TRACE_INVALID_PARSED_TOKEN("UnknownKey", t);
		return false;
	} else if (!t.issuedAtUnixTime.present()) {
		CODE_PROBE(true, "Token has no issued-at field");
		TRACE_INVALID_PARSED_TOKEN("NoIssuedAt", t);
		return false;
	} else if (!t.expiresAtUnixTime.present()) {
		CODE_PROBE(true, "Token has no expiration time");
		TRACE_INVALID_PARSED_TOKEN("NoExpirationTime", t);
		return false;
	} else if (double(t.expiresAtUnixTime.get()) <= currentTime) {
		CODE_PROBE(true, "Expired token");
		TRACE_INVALID_PARSED_TOKEN("Expired", t);
		return false;
	} else if (!t.notBeforeUnixTime.present()) {
		CODE_PROBE(true, "Token has no not-before field");
		TRACE_INVALID_PARSED_TOKEN("NoNotBefore", t);
		return false;
	} else if (double(t.notBeforeUnixTime.get()) > currentTime) {
		CODE_PROBE(true, "Token's not-before is in the future");
		TRACE_INVALID_PARSED_TOKEN("TokenNotYetValid", t);
		return false;
	} else if (!t.tenants.present()) {
		CODE_PROBE(true, "Token with no tenants");
		TRACE_INVALID_PARSED_TOKEN("NoTenants", t);
		return false;
	}
	std::tie(verifyOutcome, err) = authz::jwt::verifyToken(signInput, t, key.get());
	if (err.present()) {
		CODE_PROBE(true, "Error while verifying token");
		TRACE_INVALID_PARSED_TOKEN("ErrorWhileVerifyingToken", t).detail("ErrorDetail", err.get());
		return false;
	} else if (!verifyOutcome) {
		CODE_PROBE(true, "Token with invalid signature");
		TRACE_INVALID_PARSED_TOKEN("InvalidSignature", t);
		return false;
	} else {
		CacheEntry c;
		c.expirationTime = t.expiresAtUnixTime.get();
		c.tenants.reserve(c.arena, t.tenants.get().size());
		for (auto tenantId : t.tenants.get()) {
			c.tenants.push_back(c.arena, tenantId);
		}
		if (t.tokenId.present()) {
			c.tokenId = StringRef(c.arena, t.tokenId.get());
		}
		cache.insert(StringRef(c.arena, token), c);
		return true;
	}
}

bool TokenCacheImpl::validate(TenantId tenantId, StringRef token) {
	NetworkAddress peer = FlowTransport::transport().currentDeliveryPeerAddress();
	auto cachedEntry = cache.get(token);
	double currentTime = g_network->timer();

	if (!cachedEntry.present()) {
		if (validateAndAdd(currentTime, token, peer)) {
			cachedEntry = cache.get(token);
		} else {
			return false;
		}
	}

	ASSERT(cachedEntry.present());

	auto& entry = cachedEntry.get();
	if (entry->expirationTime < currentTime) {
		CODE_PROBE(true, "Found expired token in cache");
		TraceEvent(SevWarn, "InvalidToken"_audit).detail("From", peer).detail("Reason", "ExpiredInCache");
		return false;
	}
	bool tenantFound = false;
	for (auto const& t : entry->tenants) {
		if (t == tenantId) {
			tenantFound = true;
			break;
		}
	}
	if (!tenantFound) {
		CODE_PROBE(true, "Valid token doesn't reference tenant");
		TraceEvent(SevWarn, "InvalidToken"_audit)
		    .detail("From", peer)
		    .detail("Reason", "TenantTokenMismatch")
		    .detail("RequestedTenant", fmt::format("{:#x}", tenantId))
		    .detail("TenantsInToken", fmt::format("{:#x}", fmt::join(entry->tenants, " ")));
		return false;
	}
	// audit logging
	if (FLOW_KNOBS->AUDIT_LOGGING_ENABLED)
		logTokenUsage(currentTime, AuditEntry(peer, tenantId, *cachedEntry.get()));
	return true;
}

void TokenCacheImpl::logTokenUsage(double currentTime, AuditEntry&& entry) {
	if (currentTime > lastResetTime + FLOW_KNOBS->AUDIT_TIME_WINDOW) {
		// clear usage cache every AUDIT_TIME_WINDOW seconds
		usedTokens.clear();
		lastResetTime = currentTime;
	}
	auto [iter, inserted] = usedTokens.insert(std::move(entry));
	if (inserted) {
		// access in the context of this (client_ip, tenant, token_id) tuple hasn't been logged in current window. log
		// usage.
		CODE_PROBE(true, "Audit Logging Running");
		TraceEvent("AuditTokenUsed"_audit)
		    .detail("Client", iter->address)
		    .detail("TenantId", fmt::format("{:#x}", iter->tenantId))
		    .detail("TokenId", iter->tokenId)
		    .log();
	}
}

namespace authz::jwt {
extern TokenRef makeRandomTokenSpec(Arena&, IRandom&, authz::Algorithm);
}

TEST_CASE("/fdbrpc/authz/TokenCache/BadTokens") {
	auto const pubKeyName = "someEcPublicKey"_sr;
	auto const rsaPubKeyName = "someRsaPublicKey"_sr;
	auto privateKey = mkcert::makeEcP256();
	auto publicKey = privateKey.toPublic();
	auto rsaPrivateKey = mkcert::makeRsa4096Bit(); // to trigger unmatched sign algorithm
	auto rsaPublicKey = rsaPrivateKey.toPublic();
	std::pair<std::function<void(Arena&, IRandom&, authz::jwt::TokenRef&)>, char const*> badMutations[]{
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef&) { FlowTransport::transport().removeAllPublicKeys(); },
		    "NoKeyWithSuchName",
		},
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef& token) { token.expiresAtUnixTime.reset(); },
		    "NoExpirationTime",
		},
		{
		    [](Arena&, IRandom& rng, authz::jwt::TokenRef& token) {
		        token.expiresAtUnixTime = std::max<double>(g_network->timer() - 10 - rng.random01() * 50, 0);
		    },
		    "ExpiredToken",
		},
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef& token) { token.notBeforeUnixTime.reset(); },
		    "NoNotBefore",
		},
		{
		    [](Arena&, IRandom& rng, authz::jwt::TokenRef& token) {
		        token.notBeforeUnixTime = g_network->timer() + 10 + rng.random01() * 50;
		    },
		    "TokenNotYetValid",
		},
		{
		    [](Arena&, IRandom&, authz::jwt::TokenRef& token) { token.issuedAtUnixTime.reset(); },
		    "NoIssuedAt",
		},
		{
		    [](Arena& arena, IRandom&, authz::jwt::TokenRef& token) { token.tenants.reset(); },
		    "NoTenants",
		},
		{
		    [](Arena& arena, IRandom&, authz::jwt::TokenRef& token) {
		        TenantId* newTenants = new (arena) TenantId[1];
		        *newTenants = token.tenants.get()[0] + 1;
		        token.tenants = VectorRef<TenantId>(newTenants, 1);
		    },
		    "UnmatchedTenant",
		},
		{
		    [rsaPubKeyName](Arena& arena, IRandom&, authz::jwt::TokenRef& token) { token.keyId = rsaPubKeyName; },
		    "UnmatchedSignAlgorithm",
		},
	};
	auto const numBadMutations = sizeof(badMutations) / sizeof(badMutations[0]);
	for (auto repeat = 0; repeat < 50; repeat++) {
		auto arena = Arena();
		auto& rng = *deterministicRandom();
		auto validTokenSpec = authz::jwt::makeRandomTokenSpec(arena, rng, authz::Algorithm::ES256);
		validTokenSpec.keyId = pubKeyName;
		for (auto i = 0; i <= numBadMutations + 1; i++) {
			FlowTransport::transport().addPublicKey(pubKeyName, publicKey);
			FlowTransport::transport().addPublicKey(rsaPubKeyName, rsaPublicKey);
			auto publicKeyClearGuard = ScopeExit([]() { FlowTransport::transport().removeAllPublicKeys(); });
			auto signedToken = StringRef();
			auto tmpArena = Arena();
			if (i < numBadMutations) {
				auto [mutationFn, mutationDesc] = badMutations[i];
				auto mutatedTokenSpec = validTokenSpec;
				mutationFn(tmpArena, rng, mutatedTokenSpec);
				signedToken = authz::jwt::signToken(tmpArena, mutatedTokenSpec, privateKey);
				if (TokenCache::instance().validate(validTokenSpec.tenants.get()[0], signedToken)) {
					fmt::print("Unexpected successful validation at mutation {}, token spec: {}\n",
					           mutationDesc,
					           toStringRef(tmpArena, mutatedTokenSpec).toStringView());
					ASSERT(false);
				}
			} else if (i == numBadMutations) {
				// squeeze in a bad signature case that does not fit into mutation interface
				signedToken = authz::jwt::signToken(tmpArena, validTokenSpec, privateKey);
				signedToken.popBack();
				if (TokenCache::instance().validate(validTokenSpec.tenants.get()[0], signedToken)) {
					fmt::print("Unexpected successful validation with a token with truncated signature part\n");
					ASSERT(false);
				}
			} else {
				// test if badly base64-encoded tenant name causes validation to fail as expected
				auto signInput = authz::jwt::makeSignInput(tmpArena, validTokenSpec);
				auto b64Header = signInput.eat("."_sr);
				auto payload = base64::url::decode(tmpArena, signInput).get();
				rapidjson::Document d;
				d.Parse(reinterpret_cast<const char*>(payload.begin()), payload.size());
				ASSERT(!d.HasParseError());
				rapidjson::StringBuffer wrBuf;
				rapidjson::Writer<rapidjson::StringBuffer> wr(wrBuf);
				auto tenantsField = d.FindMember("tenants");
				ASSERT(tenantsField != d.MemberEnd());
				tenantsField->value.PushBack("ABC#", d.GetAllocator()); // inject base64-illegal character
				d.Accept(wr);
				auto b64ModifiedPayload = base64::url::encode(
				    tmpArena, StringRef(reinterpret_cast<const uint8_t*>(wrBuf.GetString()), wrBuf.GetSize()));
				signInput = b64Header.withSuffix("."_sr, tmpArena).withSuffix(b64ModifiedPayload, tmpArena);
				signedToken = authz::jwt::signToken(tmpArena, signInput, validTokenSpec.algorithm, privateKey);
				if (TokenCache::instance().validate(validTokenSpec.tenants.get()[0], signedToken)) {
					fmt::print(
					    "Unexpected successful validation of a token with tenant name containing non-base64 chars)\n");
					ASSERT(false);
				}
			}
		}
	}
	if (TokenCache::instance().validate(TenantInfo::INVALID_TENANT, StringRef())) {
		fmt::print("Unexpected successful validation of ill-formed token (no signature part)\n");
		ASSERT(false);
	}
	if (TokenCache::instance().validate(TenantInfo::INVALID_TENANT, "1111.22"_sr)) {
		fmt::print("Unexpected successful validation of ill-formed token (no signature part)\n");
		ASSERT(false);
	}
	if (TokenCache::instance().validate(TenantInfo::INVALID_TENANT, "////.////.////"_sr)) {
		fmt::print("Unexpected successful validation of unparseable token\n");
		ASSERT(false);
	}
	fmt::print("TEST OK\n");
	return Void();
}

TEST_CASE("/fdbrpc/authz/TokenCache/GoodTokens") {
	// Don't repeat because token expiry is at seconds granularity and sleeps are costly in unit tests
	state Arena arena;
	state PrivateKey privateKey = mkcert::makeEcP256();
	state StringRef pubKeyName = "somePublicKey"_sr;
	state ScopeExit<std::function<void()>> publicKeyClearGuard(
	    [pubKeyName = pubKeyName]() { FlowTransport::transport().removePublicKey(pubKeyName); });
	state authz::jwt::TokenRef tokenSpec =
	    authz::jwt::makeRandomTokenSpec(arena, *deterministicRandom(), authz::Algorithm::ES256);
	state StringRef signedToken;
	FlowTransport::transport().addPublicKey(pubKeyName, privateKey.toPublic());
	tokenSpec.expiresAtUnixTime = g_network->timer() + 2.0;
	tokenSpec.keyId = pubKeyName;
	signedToken = authz::jwt::signToken(arena, tokenSpec, privateKey);
	if (!TokenCache::instance().validate(tokenSpec.tenants.get()[0], signedToken)) {
		fmt::print("Unexpected failed token validation, token spec: {}, now: {}\n",
		           toStringRef(arena, tokenSpec).toStringView(),
		           g_network->timer());
		ASSERT(false);
	}
	wait(delay(3.5));
	if (TokenCache::instance().validate(tokenSpec.tenants.get()[0], signedToken)) {
		fmt::print(
		    "Unexpected successful token validation after supposedly expiring in cache, token spec: {}, now: {}\n",
		    toStringRef(arena, tokenSpec).toStringView(),
		    g_network->timer());
		ASSERT(false);
	}
	fmt::print("TEST OK\n");
	return Void();
}
