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
#include <fmt/format.h>
#include <string_view>
#include <type_traits>
#include <utility>
#if defined(HAVE_WOLFSSL)
#include <wolfssl/options.h>
#endif
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>
#include "fdbrpc/Base64UrlEncode.h"
#include "fdbrpc/Base64UrlDecode.h"

namespace {

// test-only constants for generating random tenant/key names
constexpr int MaxIssuerNameLenPlus1 = 25;
constexpr int MaxTenantNameLenPlus1 = 17;
constexpr int MaxKeyNameLenPlus1 = 21;

void trace(const char* type) {
	auto te = TraceEvent(SevWarnAlways, type);
	te.suppressFor(60);
	if (auto err = ::ERR_get_error()) {
		char buf[256]{
			0,
		};
		::ERR_error_string_n(err, buf, sizeof(buf));
		te.detail("OpenSSLError", static_cast<const char*>(buf));
	}
}

[[noreturn]] void traceAndThrow(const char* type) {
	trace(type);
	throw digital_signature_ops_error();
}

StringRef genRandomAlphanumStringRef(Arena& arena, IRandom& rng, int maxLenPlusOne) {
	const auto len = rng.randomInt(1, maxLenPlusOne);
	auto strRaw = new (arena) uint8_t[len];
	for (auto i = 0; i < len; i++)
		strRaw[i] = (uint8_t)rng.randomAlphaNumeric();
	return StringRef(strRaw, len);
}

} // namespace

namespace authz {

using MessageDigestMethod = const EVP_MD*;

Algorithm algorithmFromString(StringRef s) noexcept {
	if (s == "RS256"_sr)
		return Algorithm::RS256;
	else if (s == "ES256"_sr)
		return Algorithm::ES256;
	else
		return Algorithm::UNKNOWN;
}

StringRef signString(Arena& arena, StringRef string, PrivateKey privateKey, int keyAlgNid, MessageDigestMethod digest);

bool verifyStringSignature(StringRef string,
                           StringRef signature,
                           PublicKey publicKey,
                           int keyAlgNid,
                           MessageDigestMethod digest);

std::pair<int /*key algorithm nid*/, MessageDigestMethod> getMethod(Algorithm alg) {
	if (alg == Algorithm::RS256) {
		return { EVP_PKEY_RSA, ::EVP_sha256() };
	} else if (alg == Algorithm::ES256) {
		return { EVP_PKEY_EC, ::EVP_sha256() };
	} else {
		return { NID_undef, nullptr };
	}
}

std::string_view getAlgorithmName(Algorithm alg) {
	if (alg == Algorithm::RS256)
		return { "RS256" };
	else if (alg == Algorithm::ES256)
		return { "ES256" };
	else
		UNREACHABLE();
}

StringRef signString(Arena& arena, StringRef string, PrivateKey privateKey, int keyAlgNid, MessageDigestMethod digest) {
	ASSERT_NE(keyAlgNid, NID_undef);
	auto key = privateKey.nativeHandle();
	auto const privateKeyAlgNid = ::EVP_PKEY_base_id(key);
	if (privateKeyAlgNid != keyAlgNid) {
		TraceEvent(SevWarnAlways, "TokenSignAlgoMismatch")
		    .suppressFor(10)
		    .detail("ExpectedAlg", OBJ_nid2sn(keyAlgNid))
		    .detail("PublicKeyAlg", OBJ_nid2sn(privateKeyAlgNid));
		throw digital_signature_ops_error();
	}
	auto mdctx = ::EVP_MD_CTX_create();
	if (!mdctx)
		traceAndThrow("SignTokenInitFail");
	auto mdctxGuard = ScopeExit([mdctx]() { ::EVP_MD_CTX_free(mdctx); });
	if (1 != ::EVP_DigestSignInit(mdctx, nullptr, digest, nullptr, key))
		traceAndThrow("SignTokenInitFail");
	if (1 != ::EVP_DigestSignUpdate(mdctx, string.begin(), string.size()))
		traceAndThrow("SignTokenUpdateFail");
	auto sigLen = size_t{};
	if (1 != ::EVP_DigestSignFinal(mdctx, nullptr, &sigLen)) // assess the length first
		traceAndThrow("SignTokenGetSigLenFail");
	auto sigBuf = new (arena) uint8_t[sigLen];
	if (1 != ::EVP_DigestSignFinal(mdctx, sigBuf, &sigLen))
		traceAndThrow("SignTokenFinalizeFail");
	return StringRef(sigBuf, sigLen);
}

bool verifyStringSignature(StringRef string,
                           StringRef signature,
                           PublicKey publicKey,
                           int keyAlgNid,
                           MessageDigestMethod digest) {
	ASSERT_NE(keyAlgNid, NID_undef);
	auto key = publicKey.nativeHandle();
	auto const publicKeyAlgNid = ::EVP_PKEY_base_id(key);
	if (keyAlgNid != publicKeyAlgNid) {
		TraceEvent(SevWarnAlways, "TokenVerifyAlgoMismatch")
		    .suppressFor(10)
		    .detail("ExpectedAlg", OBJ_nid2sn(keyAlgNid))
		    .detail("PublicKeyAlg", OBJ_nid2sn(publicKeyAlgNid));
		return false; // public key's algorithm doesn't match string's
	}
	auto mdctx = ::EVP_MD_CTX_create();
	if (!mdctx) {
		trace("VerifyTokenInitFail");
		return false;
	}
	auto mdctxGuard = ScopeExit([mdctx]() { ::EVP_MD_CTX_free(mdctx); });
	if (1 != ::EVP_DigestVerifyInit(mdctx, nullptr, digest, nullptr, key)) {
		trace("VerifyTokenInitFail");
		return false;
	}
	if (1 != ::EVP_DigestVerifyUpdate(mdctx, string.begin(), string.size())) {
		trace("VerifyTokenUpdateFail");
		return false;
	}
	if (1 != ::EVP_DigestVerifyFinal(mdctx, signature.begin(), signature.size())) {
		auto te = TraceEvent(SevWarnAlways, "VerifyTokenFail");
		te.suppressFor(5);
		return false;
	}
	return true;
}

} // namespace authz

namespace authz::flatbuffers {

SignedTokenRef signToken(Arena& arena, TokenRef token, StringRef keyName, PrivateKey privateKey) {
	auto ret = SignedTokenRef{};
	auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
	writer.serialize(token);
	auto tokenStr = writer.toStringRef();
	auto [keyAlgNid, digest] = getMethod(Algorithm::ES256);
	auto sig = signString(arena, tokenStr, privateKey, keyAlgNid, digest);
	ret.token = tokenStr;
	ret.signature = sig;
	ret.keyName = StringRef(arena, keyName);
	return ret;
}

bool verifyToken(SignedTokenRef signedToken, PublicKey publicKey) {
	auto [keyAlgNid, digest] = getMethod(Algorithm::ES256);
	return verifyStringSignature(signedToken.token, signedToken.signature, publicKey, keyAlgNid, digest);
}

TokenRef makeRandomTokenSpec(Arena& arena, IRandom& rng) {
	auto token = TokenRef{};
	token.expiresAt = timer_monotonic() * (0.5 + rng.random01());
	const auto numTenants = rng.randomInt(1, 3);
	for (auto i = 0; i < numTenants; i++) {
		token.tenants.push_back(arena, genRandomAlphanumStringRef(arena, rng, MaxTenantNameLenPlus1));
	}
	return token;
}

} // namespace authz::flatbuffers

namespace authz::jwt {

template <class FieldType, class Writer>
void putField(Optional<FieldType> const& field, Writer& wr, const char* fieldName) {
	if (!field.present())
		return;
	wr.Key(fieldName);
	auto const& value = field.get();
	static_assert(std::is_same_v<StringRef, FieldType> || std::is_same_v<FieldType, uint64_t> ||
	              std::is_same_v<FieldType, VectorRef<StringRef>>);
	if constexpr (std::is_same_v<StringRef, FieldType>) {
		wr.String(reinterpret_cast<const char*>(value.begin()), value.size());
	} else if constexpr (std::is_same_v<FieldType, uint64_t>) {
		wr.Uint64(value);
	} else {
		wr.StartArray();
		for (auto elem : value) {
			wr.String(reinterpret_cast<const char*>(elem.begin()), elem.size());
		}
		wr.EndArray();
	}
}

StringRef makeTokenPart(Arena& arena, TokenRef tokenSpec) {
	using Buffer = rapidjson::StringBuffer;
	using Writer = rapidjson::Writer<Buffer>;
	auto headerBuffer = Buffer();
	auto payloadBuffer = Buffer();
	auto header = Writer(headerBuffer);
	auto payload = Writer(payloadBuffer);
	header.StartObject();
	header.Key("typ");
	header.String("JWT");
	header.Key("alg");
	auto algo = getAlgorithmName(tokenSpec.algorithm);
	header.String(algo.data(), algo.size());
	header.EndObject();
	payload.StartObject();
	putField(tokenSpec.issuer, payload, "iss");
	putField(tokenSpec.subject, payload, "sub");
	putField(tokenSpec.audience, payload, "aud");
	putField(tokenSpec.issuedAtUnixTime, payload, "iat");
	putField(tokenSpec.expiresAtUnixTime, payload, "exp");
	putField(tokenSpec.notBeforeUnixTime, payload, "nbf");
	putField(tokenSpec.keyId, payload, "kid");
	putField(tokenSpec.tokenId, payload, "jti");
	putField(tokenSpec.tenants, payload, "tenants");
	payload.EndObject();
	auto const headerPartLen = base64url::encodedLength(headerBuffer.GetSize());
	auto const payloadPartLen = base64url::encodedLength(payloadBuffer.GetSize());
	auto const totalLen = headerPartLen + 1 + payloadPartLen;
	auto out = new (arena) uint8_t[totalLen];
	auto cur = out;
	cur += base64url::encode(reinterpret_cast<const uint8_t*>(headerBuffer.GetString()), headerBuffer.GetSize(), cur);
	ASSERT_EQ(cur - out, headerPartLen);
	*cur++ = '.';
	cur += base64url::encode(reinterpret_cast<const uint8_t*>(payloadBuffer.GetString()), payloadBuffer.GetSize(), cur);
	ASSERT_EQ(cur - out, totalLen);
	return StringRef(out, totalLen);
}

StringRef makePlainSignature(Arena& arena, Algorithm alg, StringRef tokenPart, PrivateKey privateKey) {
	auto [keyAlgNid, digest] = getMethod(alg);
	return signString(arena, tokenPart, privateKey, keyAlgNid, digest);
}

StringRef signToken(Arena& arena, TokenRef tokenSpec, PrivateKey privateKey) {
	auto tmpArena = Arena();
	auto tokenPart = makeTokenPart(tmpArena, tokenSpec);
	auto plainSig = makePlainSignature(tmpArena, tokenSpec.algorithm, tokenPart, privateKey);
	auto const sigPartLen = base64url::encodedLength(plainSig.size());
	auto const totalLen = tokenPart.size() + 1 + sigPartLen;
	auto out = new (arena) uint8_t[totalLen];
	auto cur = out;
	::memcpy(cur, tokenPart.begin(), tokenPart.size());
	cur += tokenPart.size();
	*cur++ = '.';
	cur += base64url::encode(plainSig.begin(), plainSig.size(), cur);
	ASSERT_EQ(cur - out, totalLen);
	return StringRef(out, totalLen);
}

bool parseHeaderPart(TokenRef& token, StringRef b64urlHeader) {
	auto tmpArena = Arena();
	auto [header, valid] = base64url::decode(tmpArena, b64urlHeader);
	if (!valid)
		return false;
	auto d = rapidjson::Document();
	d.Parse(reinterpret_cast<const char*>(header.begin()), header.size());
	if (d.HasParseError()) {
		TraceEvent(SevWarnAlways, "TokenHeaderJsonParseError")
		    .suppressFor(10)
		    .detail("Header", header.toString())
		    .detail("Message", GetParseError_En(d.GetParseError()))
		    .detail("Offset", d.GetErrorOffset());
		return false;
	}
	if (d.IsObject() && d.HasMember("alg") && d.HasMember("typ")) {
		auto const& alg = d["alg"];
		auto const& typ = d["typ"];
		if (alg.IsString() && typ.IsString()) {
			auto algValue = StringRef(reinterpret_cast<const uint8_t*>(alg.GetString()), alg.GetStringLength());
			auto algType = algorithmFromString(algValue);
			if (algType == Algorithm::UNKNOWN)
				return false;
			token.algorithm = algType;
			auto typValue = StringRef(reinterpret_cast<const uint8_t*>(typ.GetString()), typ.GetStringLength());
			if (typValue != "JWT"_sr)
				return false;
			return true;
		}
	}
	return false;
}

template <class FieldType>
bool parseField(Arena& arena, Optional<FieldType>& out, const rapidjson::Document& d, const char* fieldName) {
	if (!d.HasMember(fieldName))
		return true;
	auto const& field = d[fieldName];
	static_assert(std::is_same_v<StringRef, FieldType> || std::is_same_v<FieldType, uint64_t> ||
	              std::is_same_v<FieldType, VectorRef<StringRef>>);
	if constexpr (std::is_same_v<FieldType, StringRef>) {
		if (!field.IsString())
			return false;
		out = StringRef(arena, reinterpret_cast<const uint8_t*>(field.GetString()), field.GetStringLength());
	} else if constexpr (std::is_same_v<FieldType, uint64_t>) {
		if (!field.IsUint64())
			return false;
		out = field.GetUint64();
	} else {
		if (!field.IsArray())
			return false;
		if (field.Size() > 0) {
			auto vector = new (arena) StringRef[field.Size()];
			for (auto i = 0; i < field.Size(); i++) {
				if (!field[i].IsString())
					return false;
				vector[i] = StringRef(
				    arena, reinterpret_cast<const uint8_t*>(field[i].GetString()), field[i].GetStringLength());
			}
			out = VectorRef<StringRef>(vector, field.Size());
		} else {
			out = VectorRef<StringRef>();
		}
	}
	return true;
}

bool parsePayloadPart(Arena& arena, TokenRef& token, StringRef b64urlPayload) {
	auto tmpArena = Arena();
	auto [payload, valid] = base64url::decode(tmpArena, b64urlPayload);
	auto d = rapidjson::Document();
	d.Parse(reinterpret_cast<const char*>(payload.begin()), payload.size());
	if (d.HasParseError()) {
		TraceEvent(SevWarnAlways, "TokenPayloadJsonParseError")
		    .suppressFor(10)
		    .detail("Payload", payload.toString())
		    .detail("Message", GetParseError_En(d.GetParseError()))
		    .detail("Offset", d.GetErrorOffset());
		return false;
	}
	if (!d.IsObject())
		return false;
	if (!parseField(arena, token.issuer, d, "iss"))
		return false;
	if (!parseField(arena, token.subject, d, "sub"))
		return false;
	if (!parseField(arena, token.audience, d, "aud"))
		return false;
	if (!parseField(arena, token.tokenId, d, "jti"))
		return false;
	if (!parseField(arena, token.issuedAtUnixTime, d, "iat"))
		return false;
	if (!parseField(arena, token.expiresAtUnixTime, d, "exp"))
		return false;
	if (!parseField(arena, token.notBeforeUnixTime, d, "nbf"))
		return false;
	if (!parseField(arena, token.keyId, d, "kid"))
		return false;
	if (!parseField(arena, token.tenants, d, "tenants"))
		return false;
	return true;
}

bool parseSignaturePart(Arena& arena, TokenRef& token, StringRef b64urlSignature) {
	auto [sig, valid] = base64url::decode(arena, b64urlSignature);
	if (valid)
		token.signature = sig;
	return valid;
}

bool parseToken(Arena& arena, TokenRef& token, StringRef signedToken) {
	auto b64urlHeader = signedToken.eat("."_sr);
	auto b64urlPayload = signedToken.eat("."_sr);
	auto b64urlSignature = signedToken;
	if (b64urlHeader.empty() || b64urlPayload.empty() || b64urlSignature.empty())
		return false;
	if (!parseHeaderPart(token, b64urlHeader))
		return false;
	if (!parsePayloadPart(arena, token, b64urlPayload))
		return false;
	if (!parseSignaturePart(arena, token, b64urlSignature))
		return false;
	return true;
}

bool verifyToken(StringRef signedToken, PublicKey publicKey) {
	auto arena = Arena();
	auto fullToken = signedToken;
	auto b64urlHeader = signedToken.eat("."_sr);
	auto b64urlPayload = signedToken.eat("."_sr);
	auto b64urlSignature = signedToken;
	if (b64urlHeader.empty() || b64urlPayload.empty() || b64urlSignature.empty())
		return false;
	auto b64urlTokenPart = fullToken.substr(0, b64urlHeader.size() + 1 + b64urlPayload.size());
	auto [sig, valid] = base64url::decode(arena, b64urlSignature);
	if (!valid)
		return false;
	auto parsedToken = TokenRef();
	if (!parseHeaderPart(parsedToken, b64urlHeader))
		return false;
	auto [keyAlgNid, digest] = getMethod(parsedToken.algorithm);
	return verifyStringSignature(b64urlTokenPart, sig, publicKey, keyAlgNid, digest);
}

TokenRef makeRandomTokenSpec(Arena& arena, IRandom& rng, Algorithm alg) {
	if (alg != Algorithm::ES256) {
		throw unsupported_operation();
	}
	auto ret = TokenRef{};
	ret.algorithm = alg;
	ret.issuer = genRandomAlphanumStringRef(arena, rng, MaxIssuerNameLenPlus1);
	ret.subject = genRandomAlphanumStringRef(arena, rng, MaxIssuerNameLenPlus1);
	ret.tokenId = genRandomAlphanumStringRef(arena, rng, 31);
	auto numAudience = rng.randomInt(1, 5);
	auto aud = new (arena) StringRef[numAudience];
	for (auto i = 0; i < numAudience; i++)
		aud[i] = genRandomAlphanumStringRef(arena, rng, MaxTenantNameLenPlus1);
	ret.audience = VectorRef<StringRef>(aud, numAudience);
	ret.issuedAtUnixTime = timer_int() / 1'000'000'000ul;
	ret.notBeforeUnixTime = timer_int() / 1'000'000'000ul;
	ret.expiresAtUnixTime = ret.issuedAtUnixTime.get() + rng.randomInt(360, 1080 + 1);
	ret.keyId = genRandomAlphanumStringRef(arena, rng, MaxKeyNameLenPlus1);
	auto numTenants = rng.randomInt(1, 3);
	auto tenants = new (arena) StringRef[numTenants];
	for (auto i = 0; i < numTenants; i++)
		tenants[i] = genRandomAlphanumStringRef(arena, rng, MaxTenantNameLenPlus1);
	ret.tenants = VectorRef<StringRef>(tenants, numTenants);
	return ret;
}

} // namespace authz::jwt

void forceLinkTokenSignTests() {}

TEST_CASE("/fdbrpc/TokenSign/FlatBuffer") {
	const auto numIters = 100;
	for (auto i = 0; i < numIters; i++) {
		auto arena = Arena();
		auto privateKey = mkcert::makeEcP256();
		auto& rng = *deterministicRandom();
		auto tokenSpec = authz::flatbuffers::makeRandomTokenSpec(arena, rng);
		auto keyName = genRandomAlphanumStringRef(arena, rng, MaxKeyNameLenPlus1);
		auto signedToken = authz::flatbuffers::signToken(arena, tokenSpec, keyName, privateKey);
		const auto verifyExpectOk = authz::flatbuffers::verifyToken(signedToken, privateKey.toPublicKey());
		ASSERT(verifyExpectOk);
		// try tampering with signed token by adding one more tenant
		tokenSpec.tenants.push_back(arena, genRandomAlphanumStringRef(arena, rng, MaxTenantNameLenPlus1));
		auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
		writer.serialize(tokenSpec);
		signedToken.token = writer.toStringRef();
		const auto verifyExpectFail = authz::flatbuffers::verifyToken(signedToken, privateKey.toPublicKey());
		ASSERT(!verifyExpectFail);
	}
	printf("%d runs OK\n", numIters);
	return Void();
}

TEST_CASE("/fdbrpc/TokenSign/JWT") {
	const auto numIters = 100;
	for (auto i = 0; i < numIters; i++) {
		auto arena = Arena();
		auto privateKey = mkcert::makeEcP256();
		auto& rng = *deterministicRandom();
		auto tokenSpec = authz::jwt::makeRandomTokenSpec(arena, rng, authz::Algorithm::ES256);
		auto signedToken = authz::jwt::signToken(arena, tokenSpec, privateKey);
		const auto verifyExpectOk = authz::jwt::verifyToken(signedToken, privateKey.toPublicKey());
		ASSERT(verifyExpectOk);
		auto signaturePart = signedToken;
		signaturePart.eat("."_sr);
		signaturePart.eat("."_sr);
		{
			auto parsedToken = authz::jwt::TokenRef{};
			auto tmpArena = Arena();
			auto parseOk = parseToken(tmpArena, parsedToken, signedToken);
			ASSERT(parseOk);
			ASSERT_EQ(tokenSpec.algorithm, parsedToken.algorithm);
			ASSERT(tokenSpec.issuer == parsedToken.issuer);
			ASSERT(tokenSpec.subject == parsedToken.subject);
			ASSERT(tokenSpec.tokenId == parsedToken.tokenId);
			ASSERT(tokenSpec.audience == parsedToken.audience);
			ASSERT(tokenSpec.keyId == parsedToken.keyId);
			ASSERT_EQ(tokenSpec.issuedAtUnixTime.get(), parsedToken.issuedAtUnixTime.get());
			ASSERT_EQ(tokenSpec.expiresAtUnixTime.get(), parsedToken.expiresAtUnixTime.get());
			ASSERT_EQ(tokenSpec.notBeforeUnixTime.get(), parsedToken.notBeforeUnixTime.get());
			ASSERT(tokenSpec.tenants == parsedToken.tenants);
			auto [sig, sigValid] = base64url::decode(tmpArena, signaturePart);
			ASSERT(sigValid);
			ASSERT(sig == parsedToken.signature);
		}
		// try tampering with signed token by adding one more tenant
		tokenSpec.tenants.get().push_back(arena, genRandomAlphanumStringRef(arena, rng, MaxTenantNameLenPlus1));
		auto tamperedTokenPart = makeTokenPart(arena, tokenSpec);
		auto tamperedTokenString = fmt::format("{}.{}", tamperedTokenPart.toString(), signaturePart.toString());
		const auto verifyExpectFail = authz::jwt::verifyToken(StringRef(tamperedTokenString), privateKey.toPublicKey());
		ASSERT(!verifyExpectFail);
	}
	printf("%d runs OK\n", numIters);
	return Void();
}

TEST_CASE("/fdbrpc/TokenSign/bench") {
	constexpr auto repeat = 10;
	constexpr auto numSamples = 10000;
	auto keys = std::vector<PrivateKey>(numSamples);
	auto pubKeys = std::vector<PublicKey>(numSamples);
	for (auto i = 0; i < numSamples; i++) {
		keys[i] = mkcert::makeEcP256();
		pubKeys[i] = keys[i].toPublicKey();
	}
	fmt::print("{} keys generated\n", numSamples);
	auto& rng = *deterministicRandom();
	auto arena = Arena();
	auto jwts = new (arena) StringRef[numSamples];
	auto fbs = new (arena) StringRef[numSamples];
	{
		auto tmpArena = Arena();
		for (auto i = 0; i < numSamples; i++) {
			auto jwtSpec = authz::jwt::makeRandomTokenSpec(tmpArena, rng, authz::Algorithm::ES256);
			jwts[i] = authz::jwt::signToken(arena, jwtSpec, keys[i]);
			auto fbSpec = authz::flatbuffers::makeRandomTokenSpec(tmpArena, rng);
			auto fbToken = authz::flatbuffers::signToken(tmpArena, fbSpec, "defaultKey"_sr, keys[i]);
			auto wr = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, Unversioned());
			wr.serialize(fbToken);
			fbs[i] = wr.toStringRef();
		}
	}
	fmt::print("{} FB/JWT tokens generated\n", numSamples);
	auto jwtBegin = timer_monotonic();
	for (auto rep = 0; rep < repeat; rep++) {
		for (auto i = 0; i < numSamples; i++) {
			auto verifyOk = authz::jwt::verifyToken(jwts[i], pubKeys[i]);
			ASSERT(verifyOk);
		}
	}
	auto jwtEnd = timer_monotonic();
	fmt::print("JWT:         {:.2f} OPS\n", repeat * numSamples / (jwtEnd - jwtBegin));
	auto fbBegin = timer_monotonic();
	for (auto rep = 0; rep < repeat; rep++) {
		for (auto i = 0; i < numSamples; i++) {
			auto signedToken = ObjectReader::fromStringRef<authz::flatbuffers::SignedTokenRef>(fbs[i], Unversioned());
			auto verifyOk = authz::flatbuffers::verifyToken(signedToken, pubKeys[i]);
			ASSERT(verifyOk);
		}
	}
	auto fbEnd = timer_monotonic();
	fmt::print("FlatBuffers: {:.2f} OPS\n", repeat * numSamples / (fbEnd - fbBegin));
	return Void();
}
