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

StringRef genRandomAlphanumStringRef(Arena& arena, IRandom& rng, int maxLenPlusOne) {
	const auto len = rng.randomInt(1, maxLenPlusOne);
	auto strRaw = new (arena) uint8_t[len];
	for (auto i = 0; i < len; i++)
		strRaw[i] = (uint8_t)rng.randomAlphaNumeric();
	return StringRef(strRaw, len);
}

bool checkVerifyAlgorithm(PKeyAlgorithm algo, PublicKey key) {
	if (algo != key.algorithm()) {
		TraceEvent(SevWarnAlways, "TokenVerifyAlgoMismatch")
		    .suppressFor(10)
		    .detail("Expected", pkeyAlgorithmName(algo))
		    .detail("PublicKeyAlgorithm", key.algorithmName());
		return false;
	} else {
		return true;
	}
}

bool checkSignAlgorithm(PKeyAlgorithm algo, PrivateKey key) {
	if (algo != key.algorithm()) {
		TraceEvent(SevWarnAlways, "TokenSignAlgoMismatch")
		    .suppressFor(10)
		    .detail("Expected", pkeyAlgorithmName(algo))
		    .detail("PublicKeyAlgorithm", key.algorithmName());
		return false;
	} else {
		return true;
	}
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

std::pair<PKeyAlgorithm, MessageDigestMethod> getMethod(Algorithm alg) {
	if (alg == Algorithm::RS256) {
		return { PKeyAlgorithm::RSA, ::EVP_sha256() };
	} else if (alg == Algorithm::ES256) {
		return { PKeyAlgorithm::EC, ::EVP_sha256() };
	} else {
		return { PKeyAlgorithm::UNSUPPORTED, nullptr };
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

} // namespace authz

namespace authz::flatbuffers {

SignedTokenRef signToken(Arena& arena, TokenRef token, StringRef keyName, PrivateKey privateKey) {
	auto ret = SignedTokenRef{};
	auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
	writer.serialize(token);
	auto tokenStr = writer.toStringRef();
	auto [signAlgo, digest] = getMethod(Algorithm::ES256);
	if (!checkSignAlgorithm(signAlgo, privateKey)) {
		throw digital_signature_ops_error();
	}
	auto sig = privateKey.sign(arena, tokenStr, *digest);
	ret.token = tokenStr;
	ret.signature = sig;
	ret.keyName = StringRef(arena, keyName);
	return ret;
}

bool verifyToken(SignedTokenRef signedToken, PublicKey publicKey) {
	auto [keyAlg, digest] = getMethod(Algorithm::ES256);
	if (!checkVerifyAlgorithm(keyAlg, publicKey))
		return false;
	return publicKey.verify(signedToken.token, signedToken.signature, *digest);
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

StringRef signToken(Arena& arena, TokenRef tokenSpec, PrivateKey privateKey) {
	auto tmpArena = Arena();
	auto tokenPart = makeTokenPart(tmpArena, tokenSpec);
	auto [signAlgo, digest] = getMethod(tokenSpec.algorithm);
	if (!checkSignAlgorithm(signAlgo, privateKey)) {
		throw digital_signature_ops_error();
	}
	auto plainSig = privateKey.sign(tmpArena, tokenPart, *digest);
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
	auto optHeader = base64url::decode(tmpArena, b64urlHeader);
	if (!optHeader.present())
		return false;
	auto header = optHeader.get();
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
	auto algItr = d.FindMember("alg");
	auto typItr = d.FindMember("typ");
	if (d.IsObject() && algItr != d.MemberEnd() && typItr != d.MemberEnd()) {
		auto const& alg = algItr->value;
		auto const& typ = typItr->value;
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
	auto fieldItr = d.FindMember(fieldName);
	if (fieldItr == d.MemberEnd())
		return true;
	auto const& field = fieldItr->value;
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
	auto optPayload = base64url::decode(tmpArena, b64urlPayload);
	if (!optPayload.present())
		return false;
	auto payload = optPayload.get();
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
	auto optSig = base64url::decode(arena, b64urlSignature);
	if (!optSig.present())
		return false;
	token.signature = optSig.get();
	return true;
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
	auto optSig = base64url::decode(arena, b64urlSignature);
	if (!optSig.present())
		return false;
	auto sig = optSig.get();
	auto parsedToken = TokenRef();
	if (!parseHeaderPart(parsedToken, b64urlHeader))
		return false;
	auto [verifyAlgo, digest] = getMethod(parsedToken.algorithm);
	if (!checkVerifyAlgorithm(verifyAlgo, publicKey))
		return false;
	return publicKey.verify(b64urlTokenPart, sig, *digest);
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
		const auto verifyExpectOk = authz::flatbuffers::verifyToken(signedToken, privateKey.toPublic());
		ASSERT(verifyExpectOk);
		// try tampering with signed token by adding one more tenant
		tokenSpec.tenants.push_back(arena, genRandomAlphanumStringRef(arena, rng, MaxTenantNameLenPlus1));
		auto writer = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, IncludeVersion());
		writer.serialize(tokenSpec);
		signedToken.token = writer.toStringRef();
		const auto verifyExpectFail = authz::flatbuffers::verifyToken(signedToken, privateKey.toPublic());
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
		const auto verifyExpectOk = authz::jwt::verifyToken(signedToken, privateKey.toPublic());
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
			auto optSig = base64url::decode(tmpArena, signaturePart);
			ASSERT(optSig.present());
			ASSERT(optSig.get() == parsedToken.signature);
		}
		// try tampering with signed token by adding one more tenant
		tokenSpec.tenants.get().push_back(arena, genRandomAlphanumStringRef(arena, rng, MaxTenantNameLenPlus1));
		auto tamperedTokenPart = makeTokenPart(arena, tokenSpec);
		auto tamperedTokenString = fmt::format("{}.{}", tamperedTokenPart.toString(), signaturePart.toString());
		const auto verifyExpectFail = authz::jwt::verifyToken(StringRef(tamperedTokenString), privateKey.toPublic());
		ASSERT(!verifyExpectFail);
	}
	printf("%d runs OK\n", numIters);
	return Void();
}

TEST_CASE("/fdbrpc/TokenSign/bench") {
	constexpr auto repeat = 5;
	constexpr auto numSamples = 10000;
	auto keys = std::vector<PrivateKey>(numSamples);
	auto pubKeys = std::vector<PublicKey>(numSamples);
	for (auto i = 0; i < numSamples; i++) {
		keys[i] = mkcert::makeEcP256();
		pubKeys[i] = keys[i].toPublic();
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
			auto signedToken =
			    ObjectReader::fromStringRef<Standalone<authz::flatbuffers::SignedTokenRef>>(fbs[i], Unversioned());
			auto verifyOk = authz::flatbuffers::verifyToken(signedToken, pubKeys[i]);
			ASSERT(verifyOk);
		}
	}
	auto fbEnd = timer_monotonic();
	fmt::print("FlatBuffers: {:.2f} OPS\n", repeat * numSamples / (fbEnd - fbBegin));
	return Void();
}
