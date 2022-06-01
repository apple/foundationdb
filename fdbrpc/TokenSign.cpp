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
#include "flow/MkCert.h"
#include "flow/Platform.h"
#include "flow/ScopeExit.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include <type_traits>
#if defined(HAVE_WOLFSSL)
#include <wolfssl/options.h>
#endif
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/hmac.h>
#include <fmt/format.h>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/algorithm/string.hpp>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

namespace {

[[noreturn]] void traceAndThrow(const char* type) {
	auto te = TraceEvent(SevWarnAlways, type);
	te.suppressFor(60);
	if (auto err = ::ERR_get_error()) {
		char buf[256]{
			0,
		};
		::ERR_error_string_n(err, buf, sizeof(buf));
		te.detail("OpenSSLError", static_cast<const char*>(buf));
	}
	throw digital_signature_ops_error();
}

} // namespace

StringRef s256Sign(Arena& arena, StringRef str, StringRef privateKeyDer) {
	auto rawPrivKeyDer = privateKeyDer.begin();
	auto key = ::d2i_AutoPrivateKey(nullptr, &rawPrivKeyDer, privateKeyDer.size());
	if (!key) {
		traceAndThrow("SignTokenBadKey");
	}
	auto keyGuard = ScopeExit([key]() { ::EVP_PKEY_free(key); });
	auto mdctx = ::EVP_MD_CTX_create();
	if (!mdctx)
		traceAndThrow("SignTokenInitFail");
	auto mdctxGuard = ScopeExit([mdctx]() { ::EVP_MD_CTX_free(mdctx); });
	if (1 != ::EVP_DigestSignInit(mdctx, nullptr, ::EVP_sha256() /*Parameterize?*/, nullptr, key))
		traceAndThrow("SignTokenInitFail");
	if (1 != ::EVP_DigestSignUpdate(mdctx, str.begin(), str.size()))
		traceAndThrow("SignTokenUpdateFail");
	auto sigLen = size_t{};
	if (1 != ::EVP_DigestSignFinal(mdctx, nullptr, &sigLen)) // assess the length first
		traceAndThrow("SignTokenGetSigLenFail");
	auto sigBuf = new (arena) uint8_t[sigLen];
	if (1 != ::EVP_DigestSignFinal(mdctx, sigBuf, &sigLen))
		traceAndThrow("SignTokenFinalizeFail");
	return StringRef(sigBuf, sigLen);
}

bool s256Verify(StringRef str, StringRef signature, StringRef publicKeyDer) {
	auto rawPubKeyDer = publicKeyDer.begin();
	auto key = ::d2i_PUBKEY(nullptr, &rawPubKeyDer, publicKeyDer.size());
	if (!key)
		traceAndThrow("VerifyTokenBadKey");
	auto keyGuard = ScopeExit([key]() { ::EVP_PKEY_free(key); });
	auto mdctx = ::EVP_MD_CTX_create();
	if (!mdctx)
		traceAndThrow("VerifyTokenInitFail");
	auto mdctxGuard = ScopeExit([mdctx]() { ::EVP_MD_CTX_free(mdctx); });
	if (1 != ::EVP_DigestVerifyInit(mdctx, nullptr, ::EVP_sha256(), nullptr, key))
		traceAndThrow("VerifyTokenInitFail");
	if (1 != ::EVP_DigestVerifyUpdate(mdctx, str.begin(), str.size()))
		traceAndThrow("VerifyTokenUpdateFail");
	if (1 != ::EVP_DigestVerifyFinal(mdctx, signature.begin(), signature.size())) {
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

StringRef hs256GetMac(Arena& arena, StringRef str, StringRef key) {
	unsigned int mdLength = 256 / 8;
	auto md = new (arena) uint8_t[mdLength];
	::HMAC(::EVP_sha256(), key.begin(), key.size(), str.begin(), str.size(), md, &mdLength);
	return StringRef(md, mdLength);
}

bool hmacSHA256Verify(StringRef str, StringRef signature, StringRef key) {
	Arena arena;
	auto res = hs256GetMac(arena, str, key);
	return signature == res;
}

enum class CryptAlgo { ES256, HMACSHA256 };

StringRef sign(CryptAlgo algo, Arena& arena, StringRef str, StringRef key) {
	switch (algo) {
	case CryptAlgo::ES256:
		return s256Sign(arena, str, key);
	case CryptAlgo::HMACSHA256:
		return hs256GetMac(arena, str, key);
	}
	ASSERT(false);
	return StringRef();
}

bool verify(CryptAlgo algo, StringRef str, StringRef signature, StringRef key) {
	switch (algo) {
	case CryptAlgo::ES256:
		return s256Verify(str, signature, key);
	case CryptAlgo::HMACSHA256:
		return hmacSHA256Verify(str, signature, key);
	}
	ASSERT(false);
	return false;
}

Standalone<SignedAuthTokenRef> signToken(CryptAlgo algo,
                                         AuthTokenRef token,
                                         StringRef keyName,
                                         StringRef privateKeyDer) {
	auto ret = Standalone<SignedAuthTokenRef>{};
	auto& arena = ret.arena();
	auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
	writer.serialize(token);
	auto tokenStr = writer.toStringRef();

	ret.token = tokenStr;
	ret.signature = sign(algo, arena, tokenStr, privateKeyDer);
	ret.keyName = StringRef(arena, keyName);
	return ret;
}

bool verifyToken(CryptAlgo algo, SignedAuthTokenRef signedToken, StringRef publicKeyDer) {
	return verify(algo, signedToken.token, signedToken.signature, publicKeyDer);
}

bool verifyToken(SignedAuthTokenRef signedToken, StringRef publicKeyDer) {
	return verifyToken(CryptAlgo::ES256, signedToken, publicKeyDer);
}

Standalone<SignedAuthTokenRef> signToken(AuthTokenRef token, StringRef keyName, StringRef privateKeyDer) {
	return signToken(CryptAlgo::ES256, token, keyName, privateKeyDer);
}

void forceLinkTokenSignTests() {}

TEST_CASE("/fdbrpc/TokenSign") {
	const auto numIters = 100;
	for (auto i = 0; i < numIters; i++) {
		auto kpArena = Arena();
		auto keyPair = mkcert::KeyPairRef::make(kpArena);
		auto token = Standalone<AuthTokenRef>{};
		auto& arena = token.arena();
		auto& rng = *deterministicRandom();
		token.expiresAt = timer_monotonic() * (0.5 + rng.random01());
		if (auto setIp = rng.randomInt(0, 3)) {
			if (setIp == 1) {
				token.ipAddress = IPAddress(rng.randomUInt32());
			} else {
				auto v6 = std::array<uint8_t, 16>{};
				for (auto& byte : v6)
					byte = rng.randomUInt32() & 255;
				token.ipAddress = IPAddress(v6);
			}
		}
		auto genRandomStringRef = [&arena, &rng]() {
			const auto len = rng.randomInt(1, 21);
			auto strRaw = new (arena) uint8_t[len];
			for (auto i = 0; i < len; i++)
				strRaw[i] = (uint8_t)rng.randomAlphaNumeric();
			return StringRef(strRaw, len);
		};
		const auto numTenants = rng.randomInt(0, 31);
		for (auto i = 0; i < numTenants; i++) {
			token.tenants.push_back(arena, genRandomStringRef());
		}
		auto keyName = genRandomStringRef();
		auto signedToken = signToken(token, keyName, keyPair.privateKeyDer);
		const auto verifyExpectOk = verifyToken(signedToken, keyPair.publicKeyDer);
		ASSERT(verifyExpectOk);
		// try tampering with signed token by adding one more tenant
		token.tenants.push_back(arena, genRandomStringRef());
		auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
		writer.serialize(token);
		signedToken.token = writer.toStringRef();
		const auto verifyExpectFail = verifyToken(signedToken, keyPair.publicKeyDer);
		ASSERT(!verifyExpectFail);
	}
	printf("%d runs OK\n", numIters);
	return Void();
}

namespace {

std::string decode64(const std::string& val) {
	using namespace boost::archive::iterators;
	using It = transform_width<binary_from_base64<std::string::const_iterator>, 8, 6>;
	return boost::algorithm::trim_right_copy_if(std::string(It(std::begin(val)), It(std::end(val))),
	                                            [](char c) { return c == '\0'; });
}

std::string encode64(const std::string& val) {
	using namespace boost::archive::iterators;
	using It = base64_from_binary<transform_width<std::string::const_iterator, 6, 8>>;
	auto tmp = std::string(It(std::begin(val)), It(std::end(val)));
	return tmp.append((3 - val.size() % 3) % 3, '=');
}

template <class GenFun, class ValidateFun>
void tokenVerifyBench(CryptAlgo algo, GenFun& gen, ValidateFun& validate) {
	constexpr int numTokens = 10'000;
	constexpr double testTime = 10;
	Arena arena;
	mkcert::KeyPairRef keyPair;
	switch (algo) {
	case CryptAlgo::ES256:
		keyPair = mkcert::KeyPairRef::make(arena);
		break;
	case CryptAlgo::HMACSHA256: {
		// the following is not secure, but good enough for testing
		// we create a 64 byte key
		auto k = new (arena) uint8_t[64];
		static_assert(64 % sizeof(double) == 0);
		for (int i = 0; i < 64; i += sizeof(double)) {
			double d = nondeterministicRandom()->random01();
			memcpy(k + i, &d, sizeof(double));
		}
		StringRef key(k, 64);
		keyPair.privateKeyDer = key;
		keyPair.publicKeyDer = key;
		break;
	}
	}
	std::vector<decltype(gen(algo, keyPair))> tokens;
	tokens.reserve(numTokens);
	fmt::print("Generating tokens");
	for (int i = 0; i < numTokens; ++i) {
		tokens.push_back(gen(algo, keyPair));
		if (i % 1000 == 0) {
			fmt::print(".");
		}
	}
	fmt::print("\n");
	auto startTime = timer();
	auto printTime = startTime;
	double currentTime = startTime;
	uint64_t iterations = 0;
	uint64_t lastPrintedIterations = 0;
	while (startTime + testTime > currentTime) {
		auto t = tokens[deterministicRandom()->randomInt(0, tokens.size())];
		ASSERT(validate(algo, t, keyPair.publicKeyDer));
		if (currentTime - printTime > 1.0) {
			fmt::print(
			    "{}s validate {} tokens\n", unsigned(currentTime - startTime), iterations - lastPrintedIterations);
			lastPrintedIterations = iterations;
			printTime = currentTime;
		}
		currentTime = timer();
		++iterations;
	}
	double endTime = timer();
	fmt::print("Validated {} tokens in {} seconds ({} tokens per second)\n",
	           iterations,
	           endTime - startTime,
	           unsigned(iterations / (endTime - startTime)));
}

template <class GenFun, class ValidateFun>
void tokenVerifyBench(GenFun& gen, ValidateFun& validate) {
	fmt::print("ES256:\n");
	fmt::print("======\n");
	tokenVerifyBench(CryptAlgo::ES256, gen, validate);
	fmt::print("\n");

	fmt::print("HMAC-SHA256:\n");
	fmt::print("============\n");
	tokenVerifyBench(CryptAlgo::HMACSHA256, gen, validate);
	fmt::print("\n");
}

bool verifyJWT(CryptAlgo algo, StringRef token, StringRef key) {
	StringRef fullToken = token, header = token.eat("."_sr), payload = token.eat("."_sr), signature = token,
	          headerPayload = fullToken.substr(0, fullToken.size() - signature.size() - 1);
	// Parse the json. We currently won't do anything with it, but we need to benchmark the whole thing
	rapidjson::Document headerJ, payloadJ;
	std::string headerStr = decode64(header.toString()), payloadStr = decode64(payload.toString());
	headerJ.Parse(headerStr.data(), headerStr.size());
	payloadJ.Parse(payloadStr.data(), payloadStr.size());

	std::string sig = decode64(signature.toString());
	auto res = verify(algo, headerPayload, StringRef(sig), key);
	if (!res) {
		fmt::print("Verification failed!\n");
		fmt::print("verifyJWT({})\n", fullToken.toString());
		fmt::print("verify({}, {}, {})\n",
		           algo == CryptAlgo::ES256 ? "ES256" : "HMAC-SHA256",
		           headerPayload.toString(),
		           signature.toString());
	}
	return res;
}

Standalone<StringRef> createJWT(CryptAlgo algo,
                                VectorRef<StringRef> tenants,
                                double exp,
                                StringRef keyName,
                                mkcert::KeyPairRef keys) {
	rapidjson::StringBuffer headerSS, payloadSS;
	rapidjson::Writer<rapidjson::StringBuffer> headerW(headerSS), payloadW(payloadSS);
	headerW.StartObject();

	headerW.String("typ");
	headerW.String("JWT");

	headerW.String("alg");
	headerW.String("ES256");

	headerW.EndObject();

	payloadW.StartObject();

	payloadW.String("exp");
	payloadW.Double(exp);

	payloadW.String("keyName");
	payloadW.String(reinterpret_cast<const char*>(keyName.begin()), keyName.size());

	payloadW.String("tenants");
	payloadW.StartArray();
	for (auto tenant : tenants) {
		payloadW.String(reinterpret_cast<const char*>(tenant.begin()), tenant.size());
	}
	payloadW.EndArray();

	payloadW.EndObject();

	std::string header = headerSS.GetString(), payload = payloadSS.GetString();

	auto unsignedToken = fmt::format("{}.{}", encode64(header), encode64(payload));

	Arena arena;
	auto signature = sign(algo, arena, StringRef(unsignedToken), keys.privateKeyDer).toString();
	auto res = Standalone<StringRef>(fmt::format("{}.{}", unsignedToken, encode64(signature)));
	fmt::print("JSON Header: {}\n", header);
	fmt::print("JSON payload: {}\n", payload);
	fmt::print("Public key: {}\n", encode64(keys.publicKeyDer.toString()));
	fmt::print("Private key: {}\n", encode64(keys.privateKeyDer.toString()));
	if (!verifyJWT(algo, res, keys.publicKeyDer)) {
		ASSERT(false);
	}
	return res;
}

} // namespace

TEST_CASE("performance/authz/tokenverify/jwt") {
	constexpr int numTenants = 10;
	Arena arena;
	VectorRef<StringRef> tenants;
	for (int i = 0; i < numTenants; ++i) {
		auto t = deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(4, 32));
		tenants.push_back(arena, StringRef(arena, t));
	}
	auto verifyFun = [](CryptAlgo algo, StringRef token, StringRef key) { return verifyJWT(algo, token, key); };
	auto genToken = [&tenants, &verifyFun](CryptAlgo algo, mkcert::KeyPairRef keys) {
		Arena a;
		VectorRef<StringRef> authorizedTenants;
		auto numTenants = deterministicRandom()->randomInt(1, 3);
		for (int i = 0; i < numTenants; ++i) {
			authorizedTenants.push_back(a, tenants[deterministicRandom()->randomInt(0, tenants.size())]);
		}
		auto jwt = createJWT(algo, authorizedTenants, timer() + 100.0, "defaultKey"_sr, keys);
		auto isValid = verifyFun(algo, jwt, keys.publicKeyDer);
		if (!isValid) {
			fmt::print("GENERATED INVALID TOKEN\n");
		}
		return jwt;
	};
	tokenVerifyBench(genToken, verifyFun);
	return Void();
}

TEST_CASE("performance/authz/tokenverify/flatbuffers") {
	constexpr int numTenants = 10;
	Arena arena;
	VectorRef<StringRef> tenants;
	for (int i = 0; i < numTenants; ++i) {
		auto t = deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(4, 32));
		tenants.push_back(arena, StringRef(arena, t));
	}
	auto genToken = [&tenants](CryptAlgo algo, mkcert::KeyPairRef keys) {
		Arena a;
		AuthTokenRef token;
		token.expiresAt = timer() + 100.0;
		auto numTenants = deterministicRandom()->randomInt(1, 3);
		for (int i = 0; i < numTenants; ++i) {
			token.tenants.push_back(a, tenants[deterministicRandom()->randomInt(0, tenants.size())]);
		}
		auto signedToken = signToken(token, "defaultKey"_sr, keys.privateKeyDer);
		return ObjectWriter::toValue(signedToken, AssumeVersion(g_network->protocolVersion()));
	};
	auto verifyFun = [](CryptAlgo algo, StringRef token, StringRef key) {
		auto signedToken = ObjectReader::fromStringRef<Standalone<SignedAuthTokenRef>>(
		    token, AssumeVersion(g_network->protocolVersion()));
		return verifyToken(algo, signedToken, key);
	};
	tokenVerifyBench(genToken, verifyFun);
	return Void();
}
