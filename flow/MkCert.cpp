/*
 * MkCert.cpp
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
#include "flow/IRandom.h"
#include "flow/MkCert.h"
#include "flow/PKey.h"
#include "flow/ScopeExit.h"

#include <limits>
#include <memory>
#include <string>
#include <cstring>
#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/objects.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

namespace {

[[noreturn]] void traceAndThrow(const char* condition, int line) {
	auto te = TraceEvent(SevWarnAlways, "MkCertOrKeyError");
	te.suppressFor(10).detail("Line", line).detail("Condition", condition);
	if (auto err = ::ERR_get_error()) {
		char buf[256]{
			0,
		};
		::ERR_error_string_n(err, buf, sizeof(buf));
		te.detail("OpenSSLError", static_cast<const char*>(buf));
	}
	throw tls_error();
}

} // anonymous namespace

#define OSSL_ASSERT(condition)                                                                                         \
	do {                                                                                                               \
		if (!(condition))                                                                                              \
			traceAndThrow(#condition, __LINE__);                                                                       \
	} while (false)

namespace mkcert {

// Helper functions working with OpenSSL native types
std::shared_ptr<X509> readX509CertPem(StringRef x509CertPem);
StringRef writeX509CertPem(Arena& arena, const std::shared_ptr<X509>& nativeCert);

struct CertAndKeyNative {
	std::shared_ptr<X509> cert;
	PrivateKey privateKey;
	bool valid() const noexcept { return cert && privateKey; }
	// self-signed cert case
	bool null() const noexcept { return !cert && !privateKey; }
	using SelfType = CertAndKeyNative;
	using PemType = CertAndKeyRef;

	static SelfType fromPem(PemType certAndKey) {
		auto ret = SelfType{};
		if (certAndKey.empty())
			return ret;
		auto [certPem, keyPem] = certAndKey;
		// either both set or both unset
		ASSERT(!certPem.empty() && !keyPem.empty());
		ret.cert = readX509CertPem(certPem);
		ret.privateKey = PrivateKey(PemEncoded{}, keyPem);
		return ret;
	}

	PemType toPem(Arena& arena) {
		auto ret = PemType{};
		if (null())
			return ret;
		ASSERT(valid());
		ret.certPem = writeX509CertPem(arena, cert);
		ret.privateKeyPem = privateKey.writePem(arena);
		return ret;
	}
};

CertAndKeyNative makeCertNative(CertSpecRef spec, CertAndKeyNative issuer);

void printCert(FILE* out, StringRef certPem) {
	auto x = readX509CertPem(certPem);
	OSSL_ASSERT(0 < ::X509_print_fp(out, x.get()));
}

void printPrivateKey(FILE* out, StringRef privateKeyPem) {
	auto key = PrivateKey(PemEncoded{}, privateKeyPem);
	auto bio = AutoCPointer(::BIO_new_fp(out, BIO_NOCLOSE), &::BIO_free);
	OSSL_ASSERT(bio);
	OSSL_ASSERT(0 < ::EVP_PKEY_print_private(bio, key.nativeHandle(), 0, nullptr));
}

std::shared_ptr<X509> readX509CertPem(StringRef x509CertPem) {
	ASSERT(!x509CertPem.empty());
	auto bio_mem = AutoCPointer(::BIO_new_mem_buf(x509CertPem.begin(), x509CertPem.size()), &::BIO_free);
	OSSL_ASSERT(bio_mem);
	auto ret = ::PEM_read_bio_X509(bio_mem, nullptr, nullptr, nullptr);
	OSSL_ASSERT(ret);
	return std::shared_ptr<X509>(ret, &::X509_free);
}

StringRef writeX509CertPem(Arena& arena, const std::shared_ptr<X509>& nativeCert) {
	auto mem = AutoCPointer(::BIO_new(::BIO_s_mem()), &::BIO_free);
	OSSL_ASSERT(mem);
	OSSL_ASSERT(::PEM_write_bio_X509(mem, nativeCert.get()));
	auto bioBuf = std::add_pointer_t<char>{};
	auto const len = ::BIO_get_mem_data(mem, &bioBuf);
	ASSERT_GT(len, 0);
	auto buf = new (arena) uint8_t[len];
	::memcpy(buf, bioBuf, len);
	return StringRef(buf, static_cast<int>(len));
}

PrivateKey makeEcP256() {
	auto params = AutoCPointer(nullptr, &::EVP_PKEY_free);
	{
		auto paramsRaw = std::add_pointer_t<EVP_PKEY>();
		auto pctx = AutoCPointer(::EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr), &::EVP_PKEY_CTX_free);
		OSSL_ASSERT(pctx);
		OSSL_ASSERT(0 < ::EVP_PKEY_paramgen_init(pctx));
		OSSL_ASSERT(0 < ::EVP_PKEY_CTX_set_ec_paramgen_curve_nid(pctx, NID_X9_62_prime256v1));
		OSSL_ASSERT(0 < ::EVP_PKEY_paramgen(pctx, &paramsRaw));
		OSSL_ASSERT(paramsRaw);
		params.reset(paramsRaw);
	}
	// keygen
	auto kctx = AutoCPointer(::EVP_PKEY_CTX_new(params, nullptr), &::EVP_PKEY_CTX_free);
	OSSL_ASSERT(kctx);
	auto key = AutoCPointer(nullptr, &::EVP_PKEY_free);
	auto keyRaw = std::add_pointer_t<EVP_PKEY>();
	OSSL_ASSERT(0 < ::EVP_PKEY_keygen_init(kctx));
	OSSL_ASSERT(0 < ::EVP_PKEY_keygen(kctx, &keyRaw));
	OSSL_ASSERT(keyRaw);
	key.reset(keyRaw);
	auto len = 0;
	len = ::i2d_PrivateKey(key, nullptr);
	ASSERT_LT(0, len);
	auto tmpArena = Arena();
	auto buf = new (tmpArena) uint8_t[len];
	auto out = std::add_pointer_t<uint8_t>(buf);
	len = ::i2d_PrivateKey(key, &out);
	return PrivateKey(DerEncoded{}, StringRef(buf, len));
}

PrivateKey makeRsa4096Bit() {
	auto kctx = AutoCPointer(::EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), &::EVP_PKEY_CTX_free);
	OSSL_ASSERT(kctx);
	auto key = AutoCPointer(nullptr, &::EVP_PKEY_free);
	auto keyRaw = std::add_pointer_t<EVP_PKEY>();
	OSSL_ASSERT(0 < ::EVP_PKEY_keygen_init(kctx));
	OSSL_ASSERT(0 < ::EVP_PKEY_CTX_set_rsa_keygen_bits(kctx, 4096));
	OSSL_ASSERT(0 < ::EVP_PKEY_keygen(kctx, &keyRaw));
	OSSL_ASSERT(keyRaw);
	key.reset(keyRaw);
	auto len = 0;
	len = ::i2d_PrivateKey(key, nullptr);
	ASSERT_LT(0, len);
	auto tmpArena = Arena();
	auto buf = new (tmpArena) uint8_t[len];
	auto out = std::add_pointer_t<uint8_t>(buf);
	len = ::i2d_PrivateKey(key, &out);
	return PrivateKey(DerEncoded{}, StringRef(buf, len));
}

CertAndKeyNative makeCertNative(CertSpecRef spec, CertAndKeyNative issuer) {
	// issuer key/cert must be both set or both null (self-signed case)
	ASSERT(issuer.valid() || issuer.null());

	auto const isSelfSigned = issuer.null();
	auto keypair = makeEcP256();
	auto newX = ::X509_new();
	OSSL_ASSERT(newX);
	auto x509Guard = ScopeExit([&newX]() {
		if (newX)
			::X509_free(newX);
	});
	auto smartX = std::shared_ptr<X509>(newX, &::X509_free);
	newX = nullptr;
	auto x = smartX.get();
	OSSL_ASSERT(0 < ::X509_set_version(x, 2 /*X509_VERSION_3*/));
	auto serialPtr = ::X509_get_serialNumber(x);
	OSSL_ASSERT(serialPtr);
	OSSL_ASSERT(0 < ::ASN1_INTEGER_set(serialPtr, spec.serialNumber));
	auto notBefore = ::X509_getm_notBefore(x);
	OSSL_ASSERT(notBefore);
	OSSL_ASSERT(::X509_gmtime_adj(notBefore, spec.offsetNotBefore));
	auto notAfter = ::X509_getm_notAfter(x);
	OSSL_ASSERT(notAfter);
	OSSL_ASSERT(::X509_gmtime_adj(notAfter, spec.offsetNotAfter));
	OSSL_ASSERT(0 < ::X509_set_pubkey(x, keypair.nativeHandle()));
	auto subjectName = ::X509_get_subject_name(x);
	OSSL_ASSERT(subjectName);
	for (const auto& entry : spec.subjectName) {
		// field names are expected to null-terminate
		auto fieldName = entry.field.toString();
		OSSL_ASSERT(0 <
		            ::X509_NAME_add_entry_by_txt(
		                subjectName, fieldName.c_str(), MBSTRING_ASC, entry.bytes.begin(), entry.bytes.size(), -1, 0));
	}
	auto issuerName = ::X509_get_issuer_name(x);
	OSSL_ASSERT(issuerName);
	OSSL_ASSERT(::X509_set_issuer_name(x, (isSelfSigned ? subjectName : ::X509_get_subject_name(issuer.cert.get()))));
	auto ctx = X509V3_CTX{};
	X509V3_set_ctx_nodb(&ctx);
	::X509V3_set_ctx(&ctx, (isSelfSigned ? x : issuer.cert.get()), x, nullptr, nullptr, 0);
	for (const auto& entry : spec.extensions) {
		// extension field names and values are expected to null-terminate
		auto extName = entry.field.toString();
		auto extValue = entry.bytes.toString();
		auto extNid = ::OBJ_txt2nid(extName.c_str());
		if (extNid == NID_undef) {
			TraceEvent(SevWarnAlways, "MkCertInvalidExtName").suppressFor(10).detail("Name", extName);
			throw tls_error();
		}
#ifdef OPENSSL_IS_BORINGSSL
		auto ext = ::X509V3_EXT_nconf_nid(nullptr, &ctx, extNid, const_cast<char*>(extValue.c_str()));
#else
		auto ext = ::X509V3_EXT_nconf_nid(nullptr, &ctx, extNid, extValue.c_str());
#endif
		OSSL_ASSERT(ext);
		auto extGuard = ScopeExit([ext]() { ::X509_EXTENSION_free(ext); });
		OSSL_ASSERT(::X509_add_ext(x, ext, -1));
	}
	OSSL_ASSERT(
	    ::X509_sign(x, (isSelfSigned ? keypair.nativeHandle() : issuer.privateKey.nativeHandle()), ::EVP_sha256()));
	auto ret = CertAndKeyNative{};
	ret.cert = smartX;
	ret.privateKey = keypair;
	return ret;
}

CertAndKeyRef CertAndKeyRef::make(Arena& arena, CertSpecRef spec, CertAndKeyRef issuerPem) {
	auto issuer = CertAndKeyNative::fromPem(issuerPem);
	auto newCertAndKey = makeCertNative(spec, issuer);
	return newCertAndKey.toPem(arena);
}

CertSpecRef CertSpecRef::make(Arena& arena, CertKind kind) {
	auto spec = CertSpecRef{};
	spec.serialNumber = static_cast<long>(deterministicRandom()->randomInt64(0, 1e10));
	spec.offsetNotBefore = 0; // now
	spec.offsetNotAfter = 60 * 60 * 24 * 365; // 1 year from now
	auto& subject = spec.subjectName;
	subject.push_back(arena, { "countryName"_sr, "DE"_sr });
	subject.push_back(arena, { "localityName"_sr, "Berlin"_sr });
	subject.push_back(arena, { "organizationName"_sr, "FoundationDB"_sr });
	subject.push_back(arena, { "commonName"_sr, kind.getCommonName("FDB Testing Services"_sr, arena) });
	auto& ext = spec.extensions;
	if (kind.isCA()) {
		ext.push_back(arena, { "basicConstraints"_sr, "critical, CA:TRUE"_sr });
		ext.push_back(arena, { "keyUsage"_sr, "critical, digitalSignature, keyCertSign, cRLSign"_sr });
	} else {
		ext.push_back(arena, { "basicConstraints"_sr, "critical, CA:FALSE"_sr });
		ext.push_back(arena, { "keyUsage"_sr, "critical, digitalSignature, keyEncipherment"_sr });
		ext.push_back(arena, { "extendedKeyUsage"_sr, "serverAuth, clientAuth"_sr });
	}
	ext.push_back(arena, { "subjectKeyIdentifier"_sr, "hash"_sr });
	if (!kind.isRootCA())
		ext.push_back(arena, { "authorityKeyIdentifier"_sr, "keyid, issuer"_sr });
	return spec;
}

StringRef concatCertChain(Arena& arena, CertChainRef chain) {
	auto len = 0;
	for (const auto& entry : chain) {
		len += entry.certPem.size();
	}
	if (len == 0)
		return StringRef();
	auto buf = new (arena) uint8_t[len];
	auto offset = 0;
	for (auto const& entry : chain) {
		::memcpy(&buf[offset], entry.certPem.begin(), entry.certPem.size());
		offset += entry.certPem.size();
	}
	UNSTOPPABLE_ASSERT(offset == len);
	return StringRef(buf, len);
}

CertChainRef makeCertChain(Arena& arena, VectorRef<CertSpecRef> specs, Optional<CertAndKeyRef> rootAuthority) {
	ASSERT_GT(specs.size(), 0);
	// if rootAuthority is empty, use last element in specs to make root CA
	if (!rootAuthority.present()) {
		int const chainLength = specs.size();
		auto chain = new (arena) CertAndKeyRef[chainLength];
		auto caNative = makeCertNative(specs.back(), CertAndKeyNative{} /* empty issuer == self-signed */);
		chain[chainLength - 1] = caNative.toPem(arena);
		for (auto i = chainLength - 2; i >= 0; i--) {
			auto cnkNative = makeCertNative(specs[i], caNative);
			chain[i] = cnkNative.toPem(arena);
			caNative = cnkNative;
		}
		return CertChainRef(chain, chainLength);
	} else {
		int const chainLength = specs.size() + 1; /* account for deep-copied rootAuthority */
		auto chain = new (arena) CertAndKeyRef[chainLength];
		auto caNative = CertAndKeyNative::fromPem(rootAuthority.get());
		chain[chainLength - 1] = rootAuthority.get().deepCopy(arena);
		for (auto i = chainLength - 2; i >= 0; i--) {
			auto cnkNative = makeCertNative(specs[i], caNative);
			chain[i] = cnkNative.toPem(arena);
			caNative = cnkNative;
		}
		return CertChainRef(chain, chainLength);
	}
}

VectorRef<CertSpecRef> makeCertChainSpec(Arena& arena, unsigned length, ESide side) {
	if (!length)
		return {};
	auto specs = new (arena) CertSpecRef[length];
	auto const isServerSide = side == ESide::Server;
	for (auto i = 0u; i < length; i++) {
		auto kind = CertKind{};
		if (i == 0u)
			kind = isServerSide ? CertKind(Server{}) : CertKind(Client{});
		else if (i == length - 1)
			kind = isServerSide ? CertKind(ServerRootCA{}) : CertKind(ClientRootCA{});
		else
			kind = isServerSide ? CertKind(ServerIntermediateCA{ i }) : CertKind(ClientIntermediateCA{ i });
		specs[i] = CertSpecRef::make(arena, kind);
	}
	return VectorRef<CertSpecRef>(specs, length);
}

CertChainRef makeCertChain(Arena& arena, unsigned length, ESide side) {
	if (!length)
		return {};
	// temporary arena for writing up specs
	auto tmpArena = Arena();
	auto specs = makeCertChainSpec(tmpArena, length, side);
	return makeCertChain(arena, specs, {} /*root*/);
}

StringRef CertKind::getCommonName(StringRef prefix, Arena& arena) const {
	auto const side = std::string(isClientSide() ? " Client" : " Server");
	if (isIntermediateCA()) {
		auto const level = isClientSide() ? get<ClientIntermediateCA>().level : get<ServerIntermediateCA>().level;
		return prefix.withSuffix(fmt::format("{} Intermediate {}", side, level), arena);
	} else if (isRootCA()) {
		return prefix.withSuffix(fmt::format("{} Root", side), arena);
	} else {
		return prefix.withSuffix(side, arena);
	}
}

} // namespace mkcert
