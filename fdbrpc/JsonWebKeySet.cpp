/*
 * JsonWebKeySet.cpp
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

#include "flow/Arena.h"
#include "flow/AutoCPointer.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/MkCert.h"
#include "flow/PKey.h"
#include "flow/UnitTest.h"
#include "fdbrpc/Base64Encode.h"
#include "fdbrpc/Base64Decode.h"
#include "fdbrpc/JsonWebKeySet.h"
#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/opensslv.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#if OPENSSL_VERSION_NUMBER >= 0x30000000L && !defined(_WIN32)
#define USE_V3_API 1
#else
#define USE_V3_API 0
#endif

#if USE_V3_API
#include <openssl/core_names.h>
#include <openssl/param_build.h>
#endif
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <array>
#include <string_view>
#include <type_traits>

#define JWKS_ERROR(issue, op)                                                                                          \
	TraceEvent(SevWarnAlways, "JsonWebKeySet" #op "Error").suppressFor(10).detail("Issue", issue)
#define JWKS_PARSE_ERROR(issue) JWKS_ERROR(issue, Parse)
#define JWKS_WRITE_ERROR(issue) JWKS_ERROR(issue, Write)

#define JWK_PARSE_ERROR(issue)                                                                                         \
	TraceEvent(SevWarnAlways, "JsonWebKeyParseError")                                                                  \
	    .suppressFor(10)                                                                                               \
	    .detail("Issue", issue)                                                                                        \
	    .detail("KeyIndexBase0", keyIndex)
#define JWK_WRITE_ERROR(issue)                                                                                         \
	TraceEvent(SevWarnAlways, "JsonWebKeyWriteError")                                                                  \
	    .suppressFor(10)                                                                                               \
	    .detail("Issue", issue)                                                                                        \
	    .detail("KeyName", keyName.toString())
#define JWK_ERROR_OSSL(issue, op)                                                                                      \
	do {                                                                                                               \
		char buf[256]{                                                                                                 \
			0,                                                                                                         \
		};                                                                                                             \
		if (auto err = ::ERR_get_error()) {                                                                            \
			::ERR_error_string_n(err, buf, sizeof(buf));                                                               \
		}                                                                                                              \
		JWK_##op##_ERROR(issue).detail("OpenSSLError", static_cast<char const*>(buf));                                 \
	} while (0)
#define JWK_PARSE_ERROR_OSSL(issue) JWK_ERROR_OSSL(issue, PARSE)
#define JWK_WRITE_ERROR_OSSL(issue) JWK_ERROR_OSSL(issue, WRITE)

namespace {

template <bool Required, class JsonValue>
bool getJwkStringMember(JsonValue const& value,
                        char const* memberName,
                        std::conditional_t<Required, StringRef, Optional<StringRef>>& out,
                        int keyIndex) {
	auto itr = value.FindMember(memberName);
	if (itr == value.MemberEnd()) {
		if constexpr (Required) {
			JWK_PARSE_ERROR("Missing required member").detail("Member", memberName);
			return false;
		} else {
			return true;
		}
	}
	auto const& member = itr->value;
	if (!member.IsString()) {
		JWK_PARSE_ERROR("Expected member is not a string").detail("MemberName", memberName);
		return false;
	}
	out = StringRef(reinterpret_cast<uint8_t const*>(member.GetString()), member.GetStringLength());
	return true;
}

#define DECLARE_JWK_REQUIRED_STRING_MEMBER(value, member)                                                              \
	auto member = StringRef();                                                                                         \
	if (!getJwkStringMember<true>(value, #member, member, keyIndex))                                                   \
		return {}
#define DECLARE_JWK_OPTIONAL_STRING_MEMBER(value, member)                                                              \
	auto member = Optional<StringRef>();                                                                               \
	if (!getJwkStringMember<false>(value, #member, member, keyIndex))                                                  \
		return {}

template <bool Required, class AutoPtr>
bool getJwkBigNumMember(Arena& arena,
                        std::conditional_t<Required, StringRef, Optional<StringRef>> const& b64Member,
                        AutoPtr& ptr,
                        char const* memberName,
                        char const* algorithm,
                        int keyIndex) {
	if constexpr (!Required) {
		if (!b64Member.present())
			return true;
	}
	auto data = StringRef();
	if constexpr (Required) {
		data = b64Member;
	} else {
		data = b64Member.get();
	}
	auto decoded = base64::url::decode(arena, data);
	if (!decoded.present()) {
		JWK_PARSE_ERROR("Base64URL decoding for parameter failed")
		    .detail("Algorithm", algorithm)
		    .detail("Parameter", memberName);
		return false;
	}
	data = decoded.get();
	auto bn = ::BN_bin2bn(data.begin(), data.size(), nullptr);
	if (!bn) {
		JWK_PARSE_ERROR_OSSL("BN_bin2bn");
		return false;
	}
	ptr.reset(bn);
	return true;
}

#define DECL_DECODED_BN_MEMBER_REQUIRED(member, algo)                                                                  \
	auto member = AutoCPointer(nullptr, &::BN_free);                                                                   \
	if (!getJwkBigNumMember<true /*Required*/>(arena, b64##member, member, #member, algo, keyIndex))                   \
		return {}
#define DECL_DECODED_BN_MEMBER_OPTIONAL(member, algo)                                                                  \
	auto member = AutoCPointer(nullptr, &::BN_clear_free);                                                             \
	if (!getJwkBigNumMember<false /*Required*/>(arena, b64##member, member, #member, algo, keyIndex))                  \
		return {}

#define EC_DECLARE_DECODED_REQUIRED_BN_MEMBER(member) DECL_DECODED_BN_MEMBER_REQUIRED(member, "EC")
#define EC_DECLARE_DECODED_OPTIONAL_BN_MEMBER(member) DECL_DECODED_BN_MEMBER_OPTIONAL(member, "EC")
#define RSA_DECLARE_DECODED_REQUIRED_BN_MEMBER(member) DECL_DECODED_BN_MEMBER_REQUIRED(member, "RSA")
#define RSA_DECLARE_DECODED_OPTIONAL_BN_MEMBER(member) DECL_DECODED_BN_MEMBER_OPTIONAL(member, "RSA")

StringRef bigNumToBase64Url(Arena& arena, const BIGNUM* bn) {
	auto len = BN_num_bytes(bn);
	auto buf = new (arena) uint8_t[len];
	::BN_bn2bin(bn, buf);
	return base64::url::encode(arena, StringRef(buf, len));
}

Optional<PublicOrPrivateKey> parseEcP256Key(StringRef b64x, StringRef b64y, Optional<StringRef> b64d, int keyIndex) {
	auto arena = Arena();
	EC_DECLARE_DECODED_REQUIRED_BN_MEMBER(x);
	EC_DECLARE_DECODED_REQUIRED_BN_MEMBER(y);
	EC_DECLARE_DECODED_OPTIONAL_BN_MEMBER(d);
#if USE_V3_API
	// avoid deprecated API
	auto bld = AutoCPointer(::OSSL_PARAM_BLD_new(), &::OSSL_PARAM_BLD_free);
	if (!bld) {
		JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_new() for EC");
		return {};
	}
	// since OSSL_PKEY_PARAM_EC_PUB_{X|Y} are not settable params, we'll need to build a EC_GROUP and serialize it
	auto group = AutoCPointer(::EC_GROUP_new_by_curve_name(NID_X9_62_prime256v1), &::EC_GROUP_free);
	if (!group) {
		JWK_PARSE_ERROR_OSSL("EC_GROUP_new_by_curve_name()");
		return {};
	}
	auto point = AutoCPointer(::EC_POINT_new(group), &::EC_POINT_free);
	if (!point) {
		JWK_PARSE_ERROR_OSSL("EC_POINT_new()");
		return {};
	}
	if (1 != ::EC_POINT_set_affine_coordinates(group, point, x, y, nullptr)) {
		JWK_PARSE_ERROR_OSSL("EC_POINT_set_affine_coordinates()");
		return {};
	}
	auto pointBufLen = ::EC_POINT_point2oct(group, point, POINT_CONVERSION_UNCOMPRESSED, nullptr, 0, nullptr);
	if (!pointBufLen) {
		JWK_PARSE_ERROR_OSSL("EC_POINT_point2oct() for length");
		return {};
	}
	auto pointBuf = new (arena) uint8_t[pointBufLen];
	::EC_POINT_point2oct(group, point, POINT_CONVERSION_UNCOMPRESSED, pointBuf, pointBufLen, nullptr);
	if (!::OSSL_PARAM_BLD_push_utf8_string(bld, OSSL_PKEY_PARAM_GROUP_NAME, "prime256v1", sizeof("prime256v1") - 1) ||
	    !::OSSL_PARAM_BLD_push_octet_string(bld, OSSL_PKEY_PARAM_PUB_KEY, pointBuf, pointBufLen)) {
		JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_push_*() for EC (group, point)");
		return {};
	}
	if (d && !::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_PRIV_KEY, d)) {
		JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_push_BN() for EC (d)");
		return {};
	}
	auto params = AutoCPointer(::OSSL_PARAM_BLD_to_param(bld), &OSSL_PARAM_free);
	if (!params) {
		JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_to_param() for EC");
		return {};
	}
	auto pctx = AutoCPointer(::EVP_PKEY_CTX_new_from_name(nullptr, "EC", nullptr), &::EVP_PKEY_CTX_free);
	if (!pctx) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_CTX_new_from_name(EC)");
		return {};
	}
	if (1 != ::EVP_PKEY_fromdata_init(pctx)) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_fromdata_init() for EC");
		return {};
	}
	auto pkey = std::add_pointer_t<EVP_PKEY>();
	if (1 != ::EVP_PKEY_fromdata(pctx, &pkey, (d ? EVP_PKEY_KEYPAIR : EVP_PKEY_PUBLIC_KEY), params) || !pkey) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_fromdata() for EC");
		return {};
	}
	auto pkeyAutoPtr = AutoCPointer(pkey, &::EVP_PKEY_free);
#else // USE_V3_API
	auto key = AutoCPointer(::EC_KEY_new_by_curve_name(NID_X9_62_prime256v1), &::EC_KEY_free);
	if (!key) {
		JWK_PARSE_ERROR_OSSL("EC_KEY_new()");
		return {};
	}
	if (d) {
		if (1 != ::EC_KEY_set_private_key(key, d)) {
			JWK_PARSE_ERROR_OSSL("EC_KEY_set_private_key()");
			return {};
		}
	}
	if (1 != ::EC_KEY_set_public_key_affine_coordinates(key, x, y)) {
		JWK_PARSE_ERROR_OSSL("EC_KEY_set_public_key_affine_coordinates(key, x, y)");
		return {};
	}
	auto pkey = AutoCPointer(::EVP_PKEY_new(), &::EVP_PKEY_free);
	if (!pkey) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_new() for EC");
		return {};
	}
	if (1 != EVP_PKEY_set1_EC_KEY(pkey, key)) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_set1_EC_KEY()");
		return {};
	}
#endif // USE_V3_API
	if (d) {
		auto len = ::i2d_PrivateKey(pkey, nullptr);
		if (len <= 0) {
			JWK_PARSE_ERROR_OSSL("i2d_PrivateKey() for EC");
			return {};
		}
		auto buf = new (arena) uint8_t[len];
		auto out = std::add_pointer_t<uint8_t>(buf);
		len = ::i2d_PrivateKey(pkey, &out);
		// assign through public API, even if it means some parsing overhead
		return PrivateKey(DerEncoded{}, StringRef(buf, len));
	} else {
		auto len = ::i2d_PUBKEY(pkey, nullptr);
		if (len <= 0) {
			JWK_PARSE_ERROR_OSSL("i2d_PUBKEY() for EC");
			return {};
		}
		auto buf = new (arena) uint8_t[len];
		auto out = std::add_pointer_t<uint8_t>(buf);
		len = ::i2d_PUBKEY(pkey, &out);
		// assign through public API, even if it means some parsing overhead
		return PublicKey(DerEncoded{}, StringRef(buf, len));
	}
}

Optional<PublicOrPrivateKey> parseRsaKey(StringRef b64n,
                                         StringRef b64e,
                                         Optional<StringRef> b64d,
                                         Optional<StringRef> b64p,
                                         Optional<StringRef> b64q,
                                         Optional<StringRef> b64dp,
                                         Optional<StringRef> b64dq,
                                         Optional<StringRef> b64qi,
                                         int keyIndex) {
	auto arena = Arena();
	RSA_DECLARE_DECODED_REQUIRED_BN_MEMBER(n);
	RSA_DECLARE_DECODED_REQUIRED_BN_MEMBER(e);
	RSA_DECLARE_DECODED_OPTIONAL_BN_MEMBER(d);
	RSA_DECLARE_DECODED_OPTIONAL_BN_MEMBER(p);
	RSA_DECLARE_DECODED_OPTIONAL_BN_MEMBER(q);
	RSA_DECLARE_DECODED_OPTIONAL_BN_MEMBER(dp);
	RSA_DECLARE_DECODED_OPTIONAL_BN_MEMBER(dq);
	RSA_DECLARE_DECODED_OPTIONAL_BN_MEMBER(qi);
	auto const isPublic = !d || !p || !q || !dp || !dq || !qi;
#if USE_V3_API
	// avoid deprecated, algo-specific API
	auto bld = AutoCPointer(::OSSL_PARAM_BLD_new(), &::OSSL_PARAM_BLD_free);
	if (!bld) {
		JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_new() for EC");
		return {};
	}
	if (!::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_N, n) ||
	    !::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_E, e)) {
		JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_push_BN() for RSA (n, e)");
		return {};
	}
	if (!isPublic) {
		if (!::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_D, d) ||
		    !::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_FACTOR1, p) ||
		    !::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_FACTOR2, q) ||
		    !::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_EXPONENT1, dp) ||
		    !::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_EXPONENT2, dq) ||
		    !::OSSL_PARAM_BLD_push_BN(bld, OSSL_PKEY_PARAM_RSA_COEFFICIENT1, qi)) {
			JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_push_BN() for RSA (d, p, q, dp, dq, qi)");
			return {};
		}
	}
	auto params = AutoCPointer(::OSSL_PARAM_BLD_to_param(bld), &::OSSL_PARAM_free);
	if (!params) {
		JWK_PARSE_ERROR_OSSL("OSSL_PARAM_BLD_to_param() for RSA");
		return {};
	}
	auto pctx = AutoCPointer(::EVP_PKEY_CTX_new_from_name(nullptr, "RSA", nullptr), &::EVP_PKEY_CTX_free);
	if (!pctx) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_CTX_new_from_name(RSA)");
		return {};
	}
	if (1 != ::EVP_PKEY_fromdata_init(pctx)) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_fromdata_init() for RSA");
		return {};
	}
	auto pkey = std::add_pointer_t<EVP_PKEY>();
	if (1 != ::EVP_PKEY_fromdata(pctx, &pkey, (!isPublic ? EVP_PKEY_KEYPAIR : EVP_PKEY_PUBLIC_KEY), params)) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_fromdata() for EC");
		return {};
	}
	auto pkeyAutoPtr = AutoCPointer(pkey, &::EVP_PKEY_free);
#else // USE_V3_API
	auto rsa = AutoCPointer(RSA_new(), &::RSA_free);
	if (!rsa) {
		JWK_PARSE_ERROR_OSSL("RSA_new()");
		return {};
	}
	if (1 != ::RSA_set0_key(rsa, n, e, d)) {
		JWK_PARSE_ERROR_OSSL("RSA_set0_key()");
		return {};
	}
	// set0 == ownership taken by rsa, no need to free
	n.release();
	e.release();
	d.release();
	if (!isPublic) {
		if (1 != ::RSA_set0_factors(rsa, p, q)) {
			JWK_PARSE_ERROR_OSSL("RSA_set0_factors()");
			return {};
		}
		p.release();
		q.release();
		if (1 != ::RSA_set0_crt_params(rsa, dp, dq, qi)) {
			JWK_PARSE_ERROR_OSSL("RSA_set0_crt_params()");
			return {};
		}
		dp.release();
		dq.release();
		qi.release();
	}
	auto pkey = AutoCPointer(::EVP_PKEY_new(), &::EVP_PKEY_free);
	if (!pkey) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_new() for RSA");
		return {};
	}
	if (1 != ::EVP_PKEY_set1_RSA(pkey, rsa)) {
		JWK_PARSE_ERROR_OSSL("EVP_PKEY_set1_RSA()");
		return {};
	}
#endif // USE_V3_API
	if (!isPublic) {
		auto len = ::i2d_PrivateKey(pkey, nullptr);
		if (len <= 0) {
			JWK_PARSE_ERROR_OSSL("i2d_PrivateKey() for RSA");
			return {};
		}
		auto buf = new (arena) uint8_t[len];
		auto out = std::add_pointer_t<uint8_t>(buf);
		len = ::i2d_PrivateKey(pkey, &out);
		// assign through public API, even if it means some parsing overhead
		return PrivateKey(DerEncoded{}, StringRef(buf, len));
	} else {
		auto len = ::i2d_PUBKEY(pkey, nullptr);
		if (len <= 0) {
			JWK_PARSE_ERROR_OSSL("i2d_PUBKEY() for RSA");
			return {};
		}
		auto buf = new (arena) uint8_t[len];
		auto out = std::add_pointer_t<uint8_t>(buf);
		len = ::i2d_PUBKEY(pkey, &out);
		// assign through public API, even if it means some parsing overhead
		return PublicKey(DerEncoded{}, StringRef(buf, len));
	}
}

template <class Value>
Optional<PublicOrPrivateKey> parseKey(const Value& key, StringRef kty, int keyIndex) {
	if (kty == "EC"_sr) {
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, alg);
		if (alg != "ES256"_sr) {
			JWK_PARSE_ERROR("Unsupported EC algorithm").detail("Algorithm", alg.toString());
			return {};
		}
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, crv);
		if (crv != "P-256"_sr) {
			JWK_PARSE_ERROR("Unsupported EC curve").detail("Curve", crv.toString());
			return {};
		}
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, x);
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, y);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, d);
		return parseEcP256Key(x, y, d, keyIndex);
	} else if (kty == "RSA"_sr) {
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, alg);
		if (alg != "RS256"_sr) {
			JWK_PARSE_ERROR("Unsupported RSA algorithm").detail("Algorithm", alg.toString());
			return {};
		}
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, n);
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, e);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, d);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, p);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, q);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, dp);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, dq);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, qi);
		auto privKeyArgs = 0;
		privKeyArgs += d.present();
		privKeyArgs += p.present();
		privKeyArgs += q.present();
		privKeyArgs += dp.present();
		privKeyArgs += dq.present();
		privKeyArgs += qi.present();
		if (privKeyArgs == 0 || privKeyArgs == 6) {
			return parseRsaKey(n, e, d, p, q, dp, dq, qi, keyIndex);
		} else {
			JWK_PARSE_ERROR("Private key arguments partially exist").detail("NumMissingArgs", 6 - privKeyArgs);
			return {};
		}
	} else {
		JWK_PARSE_ERROR("Unsupported key type").detail("KeyType", kty.toString());
		return {};
	}
}

bool encodeEcKey(rapidjson::Writer<rapidjson::StringBuffer>& writer,
                 StringRef keyName,
                 EVP_PKEY* pKey,
                 const bool isPublic) {
	auto arena = Arena();
	writer.StartObject();
	writer.Key("kty");
	writer.String("EC");
	writer.Key("alg");
	writer.String("ES256");
	writer.Key("kid");
	writer.String(reinterpret_cast<char const*>(keyName.begin()), keyName.size());
#if USE_V3_API
	auto curveNameBuf = std::array<char, 64>{};
	auto curveNameLen = 0ul;
	if (1 != EVP_PKEY_get_utf8_string_param(
	             pKey, OSSL_PKEY_PARAM_GROUP_NAME, curveNameBuf.begin(), sizeof(curveNameBuf), &curveNameLen)) {
		JWK_WRITE_ERROR_OSSL("Get group name from EC PKey");
		return false;
	}
	auto curveName = std::string_view(curveNameBuf.cbegin(), curveNameLen);
	if (curveName != std::string_view("prime256v1")) {
		JWK_WRITE_ERROR("Unsupported EC curve").detail("CurveName", curveName);
		return false;
	}
	writer.Key("crv");
	writer.String("P-256");
#define JWK_WRITE_BN_EC_PARAM(x, param)                                                                                \
	do {                                                                                                               \
		auto x = AutoCPointer(nullptr, &::BN_clear_free);                                                              \
		auto rawX = std::add_pointer_t<BIGNUM>();                                                                      \
		if (1 != ::EVP_PKEY_get_bn_param(pKey, param, &rawX)) {                                                        \
			JWK_WRITE_ERROR_OSSL("EVP_PKEY_get_bn_param(" #param ")");                                                 \
			return false;                                                                                              \
		}                                                                                                              \
		x.reset(rawX);                                                                                                 \
		auto b64##x = bigNumToBase64Url(arena, x);                                                                     \
		writer.Key(#x);                                                                                                \
		writer.String(reinterpret_cast<char const*>(b64##x.begin()), b64##x.size());                                   \
	} while (0)
	// Get and write affine coordinates, X and Y
	JWK_WRITE_BN_EC_PARAM(x, OSSL_PKEY_PARAM_EC_PUB_X);
	JWK_WRITE_BN_EC_PARAM(y, OSSL_PKEY_PARAM_EC_PUB_Y);
	if (!isPublic) {
		JWK_WRITE_BN_EC_PARAM(d, OSSL_PKEY_PARAM_PRIV_KEY);
	}
#undef JWK_WRITE_BN_EC_PARAM
#else // USE_V3_API
	auto ecKey = ::EVP_PKEY_get0_EC_KEY(pKey); // get0 == no refcount, no need to free
	if (!ecKey) {
		JWK_WRITE_ERROR_OSSL("Could not extract EC_KEY from EVP_PKEY");
		return false;
	}
	auto group = ::EC_KEY_get0_group(ecKey);
	if (!group) {
		JWK_WRITE_ERROR("Could not get EC_GROUP from EVP_PKEY");
		return false;
	}
	auto curveName = ::EC_GROUP_get_curve_name(group);
	if (curveName == NID_undef) {
		JWK_WRITE_ERROR("Could not match EC_GROUP to known curve");
		return false;
	}
	if (curveName != NID_X9_62_prime256v1) {
		JWK_WRITE_ERROR("Unsupported curve, expected P-256 (prime256v1)").detail("curveName", ::OBJ_nid2sn(curveName));
		return false;
	}
	writer.Key("crv");
	writer.String("P-256");
	auto point = ::EC_KEY_get0_public_key(ecKey);
	if (!point) {
		JWK_WRITE_ERROR_OSSL("EC_KEY_get0_public_key() returned null");
		return false;
	}
	auto x = AutoCPointer(::BN_new(), &::BN_free);
	if (!x) {
		JWK_WRITE_ERROR_OSSL("x = BN_new()");
		return false;
	}
	auto y = AutoCPointer(::BN_new(), &::BN_free);
	if (!y) {
		JWK_WRITE_ERROR_OSSL("y = BN_new()");
		return false;
	}
	if (1 !=
#ifdef OPENSSL_IS_BORINGSSL
	    ::EC_POINT_get_affine_coordinates_GFp(group, point, x, y, nullptr)
#else
	    ::EC_POINT_get_affine_coordinates(group, point, x, y, nullptr)
#endif
	) {
		JWK_WRITE_ERROR_OSSL("EC_POINT_get_affine_coordinates()");
		return false;
	}
	auto b64X = bigNumToBase64Url(arena, x);
	auto b64Y = bigNumToBase64Url(arena, y);
	writer.Key("x");
	writer.String(reinterpret_cast<char const*>(b64X.begin()), b64X.size());
	writer.Key("y");
	writer.String(reinterpret_cast<char const*>(b64Y.begin()), b64Y.size());
	if (!isPublic) {
		auto d = ::EC_KEY_get0_private_key(ecKey);
		if (!d) {
			JWK_WRITE_ERROR("EC_KEY_get0_private_key()");
			return false;
		}
		auto b64D = bigNumToBase64Url(arena, d);
		writer.Key("d");
		writer.String(reinterpret_cast<char const*>(b64D.begin()), b64D.size());
	}
#endif // USE_V3_API
	writer.EndObject();
	return true;
}

bool encodeRsaKey(rapidjson::Writer<rapidjson::StringBuffer>& writer,
                  StringRef keyName,
                  EVP_PKEY* pKey,
                  const bool isPublic) {
	auto arena = Arena();
	writer.StartObject();
	writer.Key("kty");
	writer.String("RSA");
	writer.Key("alg");
	writer.String("RS256");
	writer.Key("kid");
	writer.String(reinterpret_cast<char const*>(keyName.begin()), keyName.size());
#if USE_V3_API
#define JWK_WRITE_BN_RSA_PARAM_V3(x, param)                                                                            \
	do {                                                                                                               \
		auto x = AutoCPointer(nullptr, &::BN_clear_free);                                                              \
		auto rawX = std::add_pointer_t<BIGNUM>();                                                                      \
		if (1 != ::EVP_PKEY_get_bn_param(pKey, param, &rawX)) {                                                        \
			JWK_WRITE_ERROR_OSSL("EVP_PKEY_get_bn_param(" #x ")");                                                     \
			return false;                                                                                              \
		}                                                                                                              \
		x.reset(rawX);                                                                                                 \
		auto b64##x = bigNumToBase64Url(arena, x);                                                                     \
		writer.Key(#x);                                                                                                \
		writer.String(reinterpret_cast<char const*>(b64##x.begin()), b64##x.size());                                   \
	} while (0)
	JWK_WRITE_BN_RSA_PARAM_V3(n, OSSL_PKEY_PARAM_RSA_N);
	JWK_WRITE_BN_RSA_PARAM_V3(e, OSSL_PKEY_PARAM_RSA_E);
	if (!isPublic) {
		JWK_WRITE_BN_RSA_PARAM_V3(d, OSSL_PKEY_PARAM_RSA_D);
		JWK_WRITE_BN_RSA_PARAM_V3(p, OSSL_PKEY_PARAM_RSA_FACTOR1);
		JWK_WRITE_BN_RSA_PARAM_V3(q, OSSL_PKEY_PARAM_RSA_FACTOR2);
		JWK_WRITE_BN_RSA_PARAM_V3(dp, OSSL_PKEY_PARAM_RSA_EXPONENT1);
		JWK_WRITE_BN_RSA_PARAM_V3(dq, OSSL_PKEY_PARAM_RSA_EXPONENT2);
		JWK_WRITE_BN_RSA_PARAM_V3(qi, OSSL_PKEY_PARAM_RSA_COEFFICIENT1);
	}
#undef JWK_WRITE_BN_RSA_PARAM_V3
#else // USE_V3_API
#define JWK_WRITE_BN_RSA_PARAM_V1(x)                                                                                   \
	do {                                                                                                               \
		if (!x) {                                                                                                      \
			JWK_WRITE_ERROR_OSSL("RSA_get0_* returned null " #x);                                                      \
			return false;                                                                                              \
		}                                                                                                              \
		auto b64##x = bigNumToBase64Url(arena, x);                                                                     \
		writer.Key(#x);                                                                                                \
		writer.String(reinterpret_cast<char const*>(b64##x.begin()), b64##x.size());                                   \
	} while (0)
	auto rsaKey = ::EVP_PKEY_get0_RSA(pKey); // get0 == no refcount, no need to free
	if (!rsaKey) {
		JWK_WRITE_ERROR_OSSL("Could not extract RSA key from EVP_PKEY");
		return false;
	}
	auto n = std::add_pointer_t<const BIGNUM>();
	auto e = std::add_pointer_t<const BIGNUM>();
	auto d = std::add_pointer_t<const BIGNUM>();
	auto p = std::add_pointer_t<const BIGNUM>();
	auto q = std::add_pointer_t<const BIGNUM>();
	auto dp = std::add_pointer_t<const BIGNUM>();
	auto dq = std::add_pointer_t<const BIGNUM>();
	auto qi = std::add_pointer_t<const BIGNUM>();
	::RSA_get0_key(rsaKey, &n, &e, &d);
	JWK_WRITE_BN_RSA_PARAM_V1(n);
	JWK_WRITE_BN_RSA_PARAM_V1(e);
	if (!isPublic) {
		::RSA_get0_factors(rsaKey, &p, &q);
		::RSA_get0_crt_params(rsaKey, &dp, &dq, &qi);
		JWK_WRITE_BN_RSA_PARAM_V1(d);
		JWK_WRITE_BN_RSA_PARAM_V1(p);
		JWK_WRITE_BN_RSA_PARAM_V1(q);
		JWK_WRITE_BN_RSA_PARAM_V1(dp);
		JWK_WRITE_BN_RSA_PARAM_V1(dq);
		JWK_WRITE_BN_RSA_PARAM_V1(qi);
	}
#undef JWK_WRITE_BN_RSA_PARAM_V1
#endif // USE_V3_API
	writer.EndObject();
	return true;
}

// Add exactly one object to context of writer. Object shall contain JWK-encoded public or private key
bool encodeKey(rapidjson::Writer<rapidjson::StringBuffer>& writer, StringRef keyName, const PublicOrPrivateKey& key) {
	auto const isPublic = key.isPublic();
	auto pKey = std::add_pointer_t<EVP_PKEY>();
	auto alg = PKeyAlgorithm{};
	if (isPublic) {
		auto const& keyObj = key.getPublic();
		pKey = keyObj.nativeHandle();
		alg = keyObj.algorithm();
	} else {
		auto const& keyObj = key.getPrivate();
		pKey = key.getPrivate().nativeHandle();
		alg = keyObj.algorithm();
	}
	if (!pKey) {
		JWK_WRITE_ERROR("PKey object to encode is null");
		return false;
	}
	if (alg == PKeyAlgorithm::EC) {
		return encodeEcKey(writer, keyName, pKey, isPublic);
	} else if (alg == PKeyAlgorithm::RSA) {
		return encodeRsaKey(writer, keyName, pKey, isPublic);
	} else {
		JWK_WRITE_ERROR("Attempted to encode PKey with unsupported algorithm");
		return false;
	}
	return true;
}

void testPublicKey(PrivateKey (*factory)()) {
	// stringify-deserialize public key.
	// sign some data using private key to see whether deserialized public key can verify it.
	auto& rng = *deterministicRandom();
	auto pubKeyName = Standalone<StringRef>("somePublicKey"_sr);
	auto privKey = factory();
	auto pubKey = privKey.toPublic();
	auto jwks = JsonWebKeySet{};
	jwks.keys.emplace(pubKeyName, pubKey);
	auto arena = Arena();
	auto jwksStr = jwks.toStringRef(arena).get();
	fmt::print("Test JWKS: {}\n", jwksStr.toString());
	auto jwksClone = JsonWebKeySet::parse(jwksStr, {});
	ASSERT(jwksClone.present());
	auto pubKeyClone = jwksClone.get().keys[pubKeyName].getPublic();
	auto randByteStr = [&rng, &arena](int len) {
		auto buf = new (arena) uint8_t[len];
		for (auto i = 0; i < len; i++)
			buf[i] = rng.randomUInt32() % 255u;
		return StringRef(buf, len);
	};
	auto randData = randByteStr(rng.randomUInt32() % 128 + 16);
	auto signature = privKey.sign(arena, randData, *::EVP_sha256());
	ASSERT(pubKeyClone.verify(randData, signature, *::EVP_sha256()));
	const_cast<uint8_t&>(*randData.begin())++;
	ASSERT(!pubKeyClone.verify(randData, signature, *::EVP_sha256()));
	fmt::print("TESTED OK FOR OPENSSL V{} API\n", (OPENSSL_VERSION_NUMBER >> 28));
}

void testPrivateKey(PrivateKey (*factory)()) {
	// stringify-deserialize private key.
	// sign some data using deserialized private key to see whether public key can verify it.
	auto& rng = *deterministicRandom();
	auto privKeyName = Standalone<StringRef>("somePrivateKey"_sr);
	auto privKey = factory();
	auto pubKey = privKey.toPublic();
	auto jwks = JsonWebKeySet{};
	jwks.keys.emplace(privKeyName, privKey);
	auto arena = Arena();
	auto jwksStr = jwks.toStringRef(arena).get();
	fmt::print("Test JWKS: {}\n", jwksStr.toString());
	auto jwksClone = JsonWebKeySet::parse(jwksStr, {});
	ASSERT(jwksClone.present());
	auto privKeyClone = jwksClone.get().keys[privKeyName].getPrivate();
	auto randByteStr = [&rng, &arena](int len) {
		auto buf = new (arena) uint8_t[len];
		for (auto i = 0; i < len; i++)
			buf[i] = rng.randomUInt32() % 255u;
		return StringRef(buf, len);
	};
	auto randData = randByteStr(rng.randomUInt32() % 128 + 16);
	auto signature = privKeyClone.sign(arena, randData, *::EVP_sha256());
	ASSERT(pubKey.verify(randData, signature, *::EVP_sha256()));
	const_cast<uint8_t&>(*randData.begin())++;
	ASSERT(!pubKey.verify(randData, signature, *::EVP_sha256()));
	fmt::print("TESTED OK FOR OPENSSL V{} API\n", (OPENSSL_VERSION_NUMBER >> 28));
}

} // anonymous namespace

Optional<JsonWebKeySet> JsonWebKeySet::parse(StringRef jwksString, VectorRef<StringRef> allowedUses) {
	auto d = rapidjson::Document();
	d.Parse(reinterpret_cast<const char*>(jwksString.begin()), jwksString.size());
	if (d.HasParseError()) {
		JWKS_PARSE_ERROR("ParseError")
		    .detail("Message", GetParseError_En(d.GetParseError()))
		    .detail("Offset", d.GetErrorOffset());
		return {};
	}
	auto keysItr = d.FindMember("keys");
	if (!d.IsObject() || keysItr == d.MemberEnd() || !keysItr->value.IsArray()) {
		JWKS_PARSE_ERROR("JWKS must be an object and have 'keys' array member");
		return {};
	}
	auto const& keys = keysItr->value;
	auto ret = JsonWebKeySet{};
	for (auto keyIndex = 0; keyIndex < keys.Size(); keyIndex++) {
		if (!keys[keyIndex].IsObject()) {
			JWKS_PARSE_ERROR("element of 'keys' array must be an object");
			return {};
		}
		auto const& key = keys[keyIndex];
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, kty);
		DECLARE_JWK_REQUIRED_STRING_MEMBER(key, kid);
		DECLARE_JWK_OPTIONAL_STRING_MEMBER(key, use);
		if (use.present() && !allowedUses.empty()) {
			auto allowed = false;
			for (auto allowedUse : allowedUses) {
				if (allowedUse == use.get()) {
					allowed = true;
					break;
				}
			}
			if (!allowed) {
				JWK_PARSE_ERROR("Illegal optional 'use' member found").detail("Use", use.get().toString());
				return {};
			}
		}
		auto parsedKey = parseKey(key, kty, keyIndex);
		if (!parsedKey.present())
			return {};
		auto [iter, inserted] = ret.keys.insert({ Standalone<StringRef>(kid), parsedKey.get() });
		if (!inserted) {
			JWK_PARSE_ERROR("Duplicate key name").detail("KeyName", kid.toString());
			return {};
		}
	}
	return ret;
}

Optional<StringRef> JsonWebKeySet::toStringRef(Arena& arena) {
	using Buffer = rapidjson::StringBuffer;
	using Writer = rapidjson::Writer<Buffer>;
	auto buffer = Buffer();
	auto writer = Writer(buffer);
	writer.StartObject();
	writer.Key("keys");
	writer.StartArray();
	for (const auto& [keyName, key] : keys) {
		if (!encodeKey(writer, keyName, key)) {
			return {};
		}
	}
	writer.EndArray();
	writer.EndObject();
	auto buf = new (arena) uint8_t[buffer.GetSize()];
	::memcpy(buf, buffer.GetString(), buffer.GetSize());
	return StringRef(buf, buffer.GetSize());
}

void forceLinkJsonWebKeySetTests() {}

TEST_CASE("/fdbrpc/JsonWebKeySet/EC/PublicKey") {
	testPublicKey(&mkcert::makeEcP256);
	return Void();
}

TEST_CASE("/fdbrpc/JsonWebKeySet/EC/PrivateKey") {
	testPrivateKey(&mkcert::makeEcP256);
	return Void();
}

TEST_CASE("/fdbrpc/JsonWebKeySet/RSA/PublicKey") {
	testPublicKey(&mkcert::makeRsa4096Bit);
	return Void();
}

TEST_CASE("/fdbrpc/JsonWebKeySet/RSA/PrivateKey") {
	testPrivateKey(&mkcert::makeRsa4096Bit);
	return Void();
}

TEST_CASE("/fdbrpc/JsonWebKeySet/Empty") {
	auto keyset = JsonWebKeySet::parse("{\"keys\":[]}"_sr, {});
	ASSERT(keyset.present());
	ASSERT(keyset.get().keys.empty());
	return Void();
}
