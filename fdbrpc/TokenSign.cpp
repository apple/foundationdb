/*
 * TokenSign.cpp
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

#include "fdbrpc/TokenSign.h"
#include "flow/network.h"
#include "flow/serialize.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include <type_traits>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>

namespace {

template <typename Func>
class ExitGuard {
	std::decay_t<Func> fn;

public:
	ExitGuard(Func&& fn) : fn(std::forward<Func>(fn)) {}

	~ExitGuard() { fn(); }
};

[[noreturn]] void traceAndThrow(const char* type) {
	auto te = TraceEvent(SevWarnAlways, type);
	te.suppressFor(60);
	if (auto err = ::ERR_get_error()) {
		char buf[256]{
			0,
		};
		::ERR_error_string_n(err, buf, sizeof(buf));
		te.detail("OpenSSLError", buf);
	}
	throw dsa_error();
}

struct KeyPair {
	StringRef privateKey;
	StringRef publicKey;
};

Standalone<KeyPair> generateEcdsaKeyPair() {
	auto params = std::add_pointer_t<EVP_PKEY>();
	{
		auto pctx = ::EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr);
		ASSERT(pctx);
		auto cg = ExitGuard([pctx]() { ::EVP_PKEY_CTX_free(pctx); });
		ASSERT_LT(0, ::EVP_PKEY_paramgen_init(pctx));
		ASSERT_LT(0, ::EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1));
		ASSERT_LT(0, ::EVP_PKEY_paramgen(pctx, &params));
		ASSERT(params);
	}
	auto cg_params = ExitGuard([params]() { ::EVP_PKEY_free(params); });
	// keygen
	auto kctx = ::EVP_PKEY_CTX_new(params, nullptr);
	ASSERT(kctx);
	auto cg_kctx = ExitGuard([kctx]() { ::EVP_PKEY_CTX_free(kctx); });
	auto key = std::add_pointer_t<EVP_PKEY>();
	{
		ASSERT_LT(0, ::EVP_PKEY_keygen_init(kctx));
		ASSERT_LT(0, ::EVP_PKEY_keygen(kctx, &key));
	}
	ASSERT(key);
	auto cg_key = ExitGuard([key]() { ::EVP_PKEY_free(key); });

	auto ret = Standalone<KeyPair>{};
	auto& arena = ret.arena();
	{
		auto len = 0;
		len = ::i2d_PrivateKey(key, nullptr);
		ASSERT_LT(0, len);
		auto buf = new (arena) uint8_t[len];
		auto out = std::add_pointer_t<uint8_t>(buf);
		len = ::i2d_PrivateKey(key, &out);
		ret.privateKey = StringRef(buf, len);
	}
	{
		auto len = 0;
		len = ::i2d_PUBKEY(key, nullptr);
		ASSERT_LT(0, len);
		auto buf = new (arena) uint8_t[len];
		auto out = std::add_pointer_t<uint8_t>(buf);
		len = ::i2d_PUBKEY(key, &out);
		ret.publicKey = StringRef(buf, len);
	}
	return ret;
}

} // namespace

Standalone<SignedToken> signToken(Token token, StringRef keyName, StringRef privateKeyDer) {
	auto ret = Standalone<SignedToken>{};
	auto arena = ret.arena();
	auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
	writer.serialize(token);
	auto tokenstr = writer.toStringRef();

	auto p_key_der = privateKeyDer.begin();
	auto key = ::d2i_AutoPrivateKey(nullptr, &p_key_der, privateKeyDer.size());
	if (!key) {
		traceAndThrow("SignTokenBadKey");
	}
	auto guard_key = ExitGuard([key]() { ::EVP_PKEY_free(key); });
	auto mdctx = ::EVP_MD_CTX_create();
	if (!mdctx)
		traceAndThrow("SignTokenInitFail");
	auto guard_mdctx = ExitGuard([mdctx]() { ::EVP_MD_CTX_free(mdctx); });
	if (1 != ::EVP_DigestSignInit(mdctx, nullptr, ::EVP_sha256() /*Parameterize?*/, nullptr, key))
		traceAndThrow("SignTokenInitFail");
	if (1 != ::EVP_DigestSignUpdate(mdctx, tokenstr.begin(), tokenstr.size()))
		traceAndThrow("SignTokenUpdateFail");
	auto siglen = size_t{};
	if (1 != ::EVP_DigestSignFinal(mdctx, nullptr, &siglen)) // assess the length first
		traceAndThrow("SignTokenGetSigLenFail");
	auto sigbuf = new (arena) uint8_t[siglen];
	if (1 != ::EVP_DigestSignFinal(mdctx, sigbuf, &siglen))
		traceAndThrow("SignTokenFinalizeFail");
	ret.token = tokenstr;
	ret.signature = StringRef(sigbuf, siglen);
	ret.keyName = StringRef(arena, keyName);
	return ret;
}

bool verifyToken(SignedToken signedToken, StringRef publicKeyDer) {
	auto p_key_der = publicKeyDer.begin();
	auto key = ::d2i_PUBKEY(nullptr, &p_key_der, publicKeyDer.size());
	if (!key)
		traceAndThrow("VerifyTokenBadKey");
	auto guard_key = ExitGuard([key]() { ::EVP_PKEY_free(key); });
	auto mdctx = ::EVP_MD_CTX_create();
	if (!mdctx)
		traceAndThrow("VerifyTokenInitFail");
	auto guard_mdctx = ExitGuard([mdctx]() { ::EVP_MD_CTX_free(mdctx); });
	if (1 != ::EVP_DigestVerifyInit(mdctx, nullptr, ::EVP_sha256(), nullptr, key))
		traceAndThrow("VerifyTokenInitFail");
	if (1 != ::EVP_DigestVerifyUpdate(mdctx, signedToken.token.begin(), signedToken.token.size()))
		traceAndThrow("VerifyTokenUpdateFail");
	if (1 != ::EVP_DigestVerifyFinal(mdctx, signedToken.signature.begin(), signedToken.signature.size())) {
		auto te = TraceEvent(SevInfo, "VerifyTokenFail");
		te.suppressFor(30);
		if (auto err = ::ERR_get_error()) {
			char buf[256]{
				0,
			};
			::ERR_error_string_n(err, buf, sizeof(buf));
			te.detail("OpenSSLError", buf);
		}
		return false;
	}
	return true;
}

void forceLinkTokenSignTests() {}

TEST_CASE("/fdbrpc/TokenSign") {
	const auto num_iters = 100;
	for (auto i = 0; i < num_iters; i++) {
		auto key_pair = generateEcdsaKeyPair();
		auto token = Standalone<Token>{};
		auto arena = token.arena();
		auto& rng = *deterministicRandom();
		token.expiresAt = timer_monotonic() * (0.5 + rng.random01());
		auto random_stringref = [&arena, &rng]() {
			const auto len = rng.randomInt(1, 21);
			auto s_raw = new (arena) uint8_t[len];
			for (auto i = 0; i < len; i++)
				s_raw[i] = (uint8_t)rng.randomAlphaNumeric();
			return StringRef(s_raw, len);
		};
		const auto num_tenants = rng.randomInt(0, 31);
		for (auto i = 0; i < num_tenants; i++) {
			token.tenants.push_back(arena, random_stringref());
		}
		auto key_name = random_stringref();
		auto signed_token = signToken(token, key_name, key_pair.privateKey);
		const auto verify_expect_ok = verifyToken(signed_token, key_pair.publicKey);
		ASSERT(verify_expect_ok);
		// try tampering with signed token by adding one more tenant
		token.tenants.push_back(arena, random_stringref());
		auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
		writer.serialize(token);
		signed_token.token = writer.toStringRef();
		const auto verify_expect_fail = verifyToken(signed_token, key_pair.publicKey);
		ASSERT(!verify_expect_fail);
	}
	printf("%d runs OK\n", num_iters);
	return Void();
}
