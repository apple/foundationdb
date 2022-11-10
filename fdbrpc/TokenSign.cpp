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
#include "flow/AutoCPointer.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/MkCert.h"
#include "flow/Platform.h"
#include "flow/ScopeExit.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include <fmt/format.h>
#include <cmath>
#include <iterator>
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
constexpr int MinIssuerNameLen = 16;
constexpr int MaxIssuerNameLenPlus1 = 25;
constexpr int MinTenantNameLen = 8;
constexpr int MaxTenantNameLenPlus1 = 17;
constexpr int MinKeyNameLen = 10;
constexpr int MaxKeyNameLenPlus1 = 21;

StringRef genRandomAlphanumStringRef(Arena& arena, IRandom& rng, int minLen, int maxLenPlusOne) {
	const auto len = rng.randomInt(minLen, maxLenPlusOne);
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

Optional<StringRef> convertEs256P1363ToDer(Arena& arena, StringRef p1363) {
	const int SIGLEN = p1363.size();
	const int HALF_SIGLEN = SIGLEN / 2;
	auto r = AutoCPointer(BN_bin2bn(p1363.begin(), HALF_SIGLEN, nullptr), &::BN_free);
	auto s = AutoCPointer(BN_bin2bn(p1363.begin() + HALF_SIGLEN, HALF_SIGLEN, nullptr), &::BN_free);
	if (!r || !s)
		return {};
	auto sig = AutoCPointer(::ECDSA_SIG_new(), &ECDSA_SIG_free);
	if (!sig)
		return {};
	::ECDSA_SIG_set0(sig, r.release(), s.release());
	auto const derLen = ::i2d_ECDSA_SIG(sig, nullptr);
	if (derLen < 0)
		return {};
	auto buf = new (arena) uint8_t[derLen];
	auto bufPtr = buf;
	::i2d_ECDSA_SIG(sig, &bufPtr);
	return StringRef(buf, derLen);
}

Optional<StringRef> convertEs256DerToP1363(Arena& arena, StringRef der) {
	uint8_t const* derPtr = der.begin();
	auto sig = AutoCPointer(::d2i_ECDSA_SIG(nullptr, &derPtr, der.size()), &::ECDSA_SIG_free);
	if (!sig) {
		return {};
	}
	// ES256-specific constant. Adapt as needed
	constexpr const int SIGLEN = 64;
	constexpr const int HALF_SIGLEN = SIGLEN / 2;
	auto buf = new (arena) uint8_t[SIGLEN];
	::memset(buf, 0, SIGLEN);
	auto bufr = buf;
	auto bufs = bufr + HALF_SIGLEN;
	auto r = std::add_pointer_t<BIGNUM const>();
	auto s = std::add_pointer_t<BIGNUM const>();
	ECDSA_SIG_get0(sig, &r, &s);
	auto const lenr = BN_num_bytes(r);
	auto const lens = BN_num_bytes(s);
	if (lenr > HALF_SIGLEN || lens > HALF_SIGLEN)
		return {};
	BN_bn2bin(r, bufr + (HALF_SIGLEN - lenr));
	BN_bn2bin(s, bufs + (HALF_SIGLEN - lens));
	return StringRef(buf, SIGLEN);
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
	auto sig = privateKey.sign(arena, tokenStr, *::EVP_sha256());
	ret.token = tokenStr;
	ret.signature = sig;
	ret.keyName = StringRef(arena, keyName);
	return ret;
}

bool verifyToken(SignedTokenRef signedToken, PublicKey publicKey) {
	return publicKey.verify(signedToken.token, signedToken.signature, *::EVP_sha256());
}

TokenRef makeRandomTokenSpec(Arena& arena, IRandom& rng) {
	auto token = TokenRef{};
	token.expiresAt = timer_monotonic() * (0.5 + rng.random01());
	const auto numTenants = rng.randomInt(1, 3);
	for (auto i = 0; i < numTenants; i++) {
		token.tenants.push_back(arena, genRandomAlphanumStringRef(arena, rng, MinTenantNameLen, MaxTenantNameLenPlus1));
	}
	return token;
}

} // namespace authz::flatbuffers

namespace authz::jwt {

template <class FieldType, size_t NameLen>
void appendField(fmt::memory_buffer& b, char const (&name)[NameLen], Optional<FieldType> const& field) {
	if (!field.present())
		return;
	auto const& f = field.get();
	auto bi = std::back_inserter(b);
	if constexpr (std::is_same_v<FieldType, VectorRef<StringRef>>) {
		fmt::format_to(bi, " {}=[", name);
		for (auto i = 0; i < f.size(); i++) {
			if (i)
				fmt::format_to(bi, ",");
			fmt::format_to(bi, f[i].toStringView());
		}
		fmt::format_to(bi, "]");
	} else if constexpr (std::is_same_v<FieldType, StringRef>) {
		fmt::format_to(bi, " {}={}", name, f.toStringView());
	} else {
		fmt::format_to(bi, " {}={}", name, f);
	}
}

StringRef TokenRef::toStringRef(Arena& arena) {
	auto buf = fmt::memory_buffer();
	fmt::format_to(std::back_inserter(buf), "alg={} kid={}", getAlgorithmName(algorithm), keyId.toStringView());
	appendField(buf, "iss", issuer);
	appendField(buf, "sub", subject);
	appendField(buf, "aud", audience);
	appendField(buf, "iat", issuedAtUnixTime);
	appendField(buf, "exp", expiresAtUnixTime);
	appendField(buf, "nbf", notBeforeUnixTime);
	appendField(buf, "jti", tokenId);
	appendField(buf, "tenants", tenants);
	auto str = new (arena) uint8_t[buf.size()];
	memcpy(str, buf.data(), buf.size());
	return StringRef(str, buf.size());
}

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
	auto algo = getAlgorithmName(tokenSpec.algorithm);
	header.Key("alg");
	header.String(algo.data(), algo.size());
	auto kid = tokenSpec.keyId.toStringView();
	header.Key("kid");
	header.String(kid.data(), kid.size());
	header.EndObject();
	payload.StartObject();
	putField(tokenSpec.issuer, payload, "iss");
	putField(tokenSpec.subject, payload, "sub");
	putField(tokenSpec.audience, payload, "aud");
	putField(tokenSpec.issuedAtUnixTime, payload, "iat");
	putField(tokenSpec.expiresAtUnixTime, payload, "exp");
	putField(tokenSpec.notBeforeUnixTime, payload, "nbf");
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
	if (tokenSpec.algorithm == Algorithm::ES256) {
		// Need to convert ASN.1/DER signature to IEEE-P1363
		auto convertedSig = convertEs256DerToP1363(tmpArena, plainSig);
		if (!convertedSig.present()) {
			auto tmpArena = Arena();
			TraceEvent(SevWarn, "TokenSigConversionFailure")
			    .detail("TokenSpec", tokenSpec.toStringRef(tmpArena).toString());
			throw digital_signature_ops_error();
		}
		plainSig = convertedSig.get();
	}
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

bool parseHeaderPart(Arena& arena, TokenRef& token, StringRef b64urlHeader) {
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
	if (!d.IsObject())
		return false;
	auto typItr = d.FindMember("typ");
	if (typItr == d.MemberEnd() || !typItr->value.IsString())
		return false;
	auto algItr = d.FindMember("alg");
	if (algItr == d.MemberEnd() || !algItr->value.IsString())
		return false;
	auto kidItr = d.FindMember("kid");
	if (kidItr == d.MemberEnd() || !kidItr->value.IsString())
		return false;
	auto const& typ = typItr->value;
	auto const& alg = algItr->value;
	auto const& kid = kidItr->value;
	auto typValue = StringRef(reinterpret_cast<const uint8_t*>(typ.GetString()), typ.GetStringLength());
	if (typValue != "JWT"_sr)
		return false;
	auto algValue = StringRef(reinterpret_cast<const uint8_t*>(alg.GetString()), alg.GetStringLength());
	auto algType = algorithmFromString(algValue);
	if (algType == Algorithm::UNKNOWN)
		return false;
	token.algorithm = algType;
	token.keyId = StringRef(arena, reinterpret_cast<const uint8_t*>(kid.GetString()), kid.GetStringLength());
	return true;
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
		if (!field.IsNumber())
			return false;
		out = static_cast<uint64_t>(field.GetDouble());
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

StringRef signaturePart(StringRef token) {
	token.eat("."_sr);
	token.eat("."_sr);
	return token;
}

bool parseToken(Arena& arena, TokenRef& token, StringRef signedToken) {
	auto b64urlHeader = signedToken.eat("."_sr);
	auto b64urlPayload = signedToken.eat("."_sr);
	auto b64urlSignature = signedToken;
	if (b64urlHeader.empty() || b64urlPayload.empty() || b64urlSignature.empty())
		return false;
	if (!parseHeaderPart(arena, token, b64urlHeader))
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
	if (!parseHeaderPart(arena, parsedToken, b64urlHeader))
		return false;
	auto [verifyAlgo, digest] = getMethod(parsedToken.algorithm);
	if (!checkVerifyAlgorithm(verifyAlgo, publicKey))
		return false;
	if (parsedToken.algorithm == Algorithm::ES256) {
		// Need to convert IEEE-P1363 signature to ASN.1/DER
		auto convertedSig = convertEs256P1363ToDer(arena, sig);
		if (!convertedSig.present())
			return false;
		sig = convertedSig.get();
	}
	return publicKey.verify(b64urlTokenPart, sig, *digest);
}

TokenRef makeRandomTokenSpec(Arena& arena, IRandom& rng, Algorithm alg) {
	auto ret = TokenRef{};
	ret.algorithm = alg;
	ret.keyId = genRandomAlphanumStringRef(arena, rng, MinKeyNameLen, MaxKeyNameLenPlus1);
	ret.issuer = genRandomAlphanumStringRef(arena, rng, MinIssuerNameLen, MaxIssuerNameLenPlus1);
	ret.subject = genRandomAlphanumStringRef(arena, rng, MinIssuerNameLen, MaxIssuerNameLenPlus1);
	ret.tokenId = genRandomAlphanumStringRef(arena, rng, 16, 31);
	auto numAudience = rng.randomInt(1, 5);
	auto aud = new (arena) StringRef[numAudience];
	for (auto i = 0; i < numAudience; i++)
		aud[i] = genRandomAlphanumStringRef(arena, rng, MinTenantNameLen, MaxTenantNameLenPlus1);
	ret.audience = VectorRef<StringRef>(aud, numAudience);
	ret.issuedAtUnixTime = g_network->timer();
	ret.notBeforeUnixTime = ret.issuedAtUnixTime.get();
	ret.expiresAtUnixTime = ret.issuedAtUnixTime.get() + rng.randomInt(360, 1080 + 1);
	auto numTenants = rng.randomInt(1, 3);
	auto tenants = new (arena) StringRef[numTenants];
	for (auto i = 0; i < numTenants; i++)
		tenants[i] = genRandomAlphanumStringRef(arena, rng, MinTenantNameLen, MaxTenantNameLenPlus1);
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
		auto keyName = genRandomAlphanumStringRef(arena, rng, MinKeyNameLen, MaxKeyNameLenPlus1);
		auto signedToken = authz::flatbuffers::signToken(arena, tokenSpec, keyName, privateKey);
		const auto verifyExpectOk = authz::flatbuffers::verifyToken(signedToken, privateKey.toPublic());
		ASSERT(verifyExpectOk);
		// try tampering with signed token by adding one more tenant
		tokenSpec.tenants.push_back(arena,
		                            genRandomAlphanumStringRef(arena, rng, MinTenantNameLen, MaxTenantNameLenPlus1));
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
		tokenSpec.tenants.get().push_back(
		    arena, genRandomAlphanumStringRef(arena, rng, MinTenantNameLen, MaxTenantNameLenPlus1));
		auto tamperedTokenPart = makeTokenPart(arena, tokenSpec);
		auto tamperedTokenString = fmt::format("{}.{}", tamperedTokenPart.toString(), signaturePart.toString());
		const auto verifyExpectFail = authz::jwt::verifyToken(StringRef(tamperedTokenString), privateKey.toPublic());
		ASSERT(!verifyExpectFail);
	}
	printf("%d runs OK\n", numIters);
	return Void();
}

TEST_CASE("/fdbrpc/TokenSign/JWT/ToStringRef") {
	auto t = authz::jwt::TokenRef();
	t.algorithm = authz::Algorithm::ES256;
	t.issuer = "issuer"_sr;
	t.subject = "subject"_sr;
	StringRef aud[3]{ "aud1"_sr, "aud2"_sr, "aud3"_sr };
	t.audience = VectorRef<StringRef>(aud, 3);
	t.issuedAtUnixTime = 123ul;
	t.expiresAtUnixTime = 456ul;
	t.notBeforeUnixTime = 789ul;
	t.keyId = "keyId"_sr;
	t.tokenId = "tokenId"_sr;
	StringRef tenants[2]{ "tenant1"_sr, "tenant2"_sr };
	t.tenants = VectorRef<StringRef>(tenants, 2);
	auto arena = Arena();
	auto tokenStr = t.toStringRef(arena);
	auto tokenStrExpected =
	    "alg=ES256 kid=keyId iss=issuer sub=subject aud=[aud1,aud2,aud3] iat=123 exp=456 nbf=789 jti=tokenId tenants=[tenant1,tenant2]"_sr;
	if (tokenStr != tokenStrExpected) {
		fmt::print("Expected: {}\nGot     : {}\n", tokenStrExpected.toStringView(), tokenStr.toStringView());
		ASSERT(false);
	} else {
		fmt::print("TEST OK\n");
	}
	return Void();
}

// This unit test takes too long to run in RandomUnitTests.toml
// FIXME: Move this to benchmark to flowbench
/*
TEST_CASE("/fdbrpc/TokenSign/bench") {
    auto keyTypes = std::array<StringRef, 2>{ "EC"_sr, "RSA"_sr };
    for (auto kty : keyTypes) {
        constexpr auto repeat = 5;
        constexpr auto numSamples = 10000;
        fmt::print("=== {} keys case\n", kty.toString());
        auto key = kty == "EC"_sr ? mkcert::makeEcP256() : mkcert::makeRsa4096Bit();
        auto pubKey = key.toPublic();
        auto& rng = *deterministicRandom();
        auto arena = Arena();
        auto jwtSpecs = new (arena) authz::jwt::TokenRef[numSamples];
        auto fbSpecs = new (arena) authz::flowserializer::TokenRef[numSamples];
        auto jwts = new (arena) StringRef[numSamples];
        auto fbs = new (arena) StringRef[numSamples];
        for (auto i = 0; i < numSamples; i++) {
            jwtSpecs[i] = authz::jwt::makeRandomTokenSpec(
                arena, rng, kty == "EC"_sr ? authz::Algorithm::ES256 : authz::Algorithm::RS256);
            fbSpecs[i] = authz::flowserializer::makeRandomTokenSpec(arena, rng);
        }
        {
            auto const jwtSignBegin = timer_monotonic();
            for (auto i = 0; i < numSamples; i++) {
                jwts[i] = authz::jwt::signToken(arena, jwtSpecs[i], key);
            }
            auto const jwtSignEnd = timer_monotonic();
            fmt::print("JWT Sign   :         {:.2f} OPS\n", numSamples / (jwtSignEnd - jwtSignBegin));
        }
        {
            auto const jwtVerifyBegin = timer_monotonic();
            for (auto rep = 0; rep < repeat; rep++) {
                for (auto i = 0; i < numSamples; i++) {
                    auto verifyOk = authz::jwt::verifyToken(jwts[i], pubKey);
                    ASSERT(verifyOk);
                }
            }
            auto const jwtVerifyEnd = timer_monotonic();
            fmt::print("JWT Verify :         {:.2f} OPS\n", repeat * numSamples / (jwtVerifyEnd - jwtVerifyBegin));
        }
        {
            auto tmpArena = Arena();
            auto const fbSignBegin = timer_monotonic();
            for (auto i = 0; i < numSamples; i++) {
                auto fbToken = authz::flowserializer::signToken(tmpArena, fbSpecs[i], "defaultKey"_sr, key);
                auto wr = ObjectWriter([&arena](size_t len) { return new (arena) uint8_t[len]; }, Unversioned());
                wr.serialize(fbToken);
                fbs[i] = wr.toStringRef();
            }
            auto const fbSignEnd = timer_monotonic();
            fmt::print("FlatBuffers Sign   : {:.2f} OPS\n", numSamples / (fbSignEnd - fbSignBegin));
        }
        {
            auto const fbVerifyBegin = timer_monotonic();
            for (auto rep = 0; rep < repeat; rep++) {
                for (auto i = 0; i < numSamples; i++) {
                    auto signedToken = ObjectReader::fromStringRef<Standalone<authz::flowserializer::SignedTokenRef>>(
                        fbs[i], Unversioned());
                    auto verifyOk = authz::flowserializer::verifyToken(signedToken, pubKey);
                    ASSERT(verifyOk);
                }
            }
            auto const fbVerifyEnd = timer_monotonic();
            fmt::print("FlatBuffers Verify : {:.2f} OPS\n", repeat * numSamples / (fbVerifyEnd - fbVerifyBegin));
        }
    }
    return Void();
}
*/
