/*
 * PKey.h
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

#include "flow/PKey.h"

#include "flow/AutoCPointer.h"
#include "flow/Error.h"
#include "flow/Trace.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/objects.h>
#include <openssl/opensslv.h>

namespace {

void traceAndThrowDecode(const char* type) {
	auto te = TraceEvent(SevWarnAlways, type);
	te.suppressFor(10);
	if (auto err = ::ERR_get_error()) {
		char buf[256]{
			0,
		};
		::ERR_error_string_n(err, buf, sizeof(buf));
		te.detail("OpenSSLError", static_cast<const char*>(buf));
	}
	throw pkey_decode_error();
}

void traceAndThrowEncode(const char* type) {
	auto te = TraceEvent(SevWarnAlways, type);
	te.suppressFor(10);
	if (auto err = ::ERR_get_error()) {
		char buf[256]{
			0,
		};
		::ERR_error_string_n(err, buf, sizeof(buf));
		te.detail("OpenSSLError", static_cast<const char*>(buf));
	}
	throw pkey_encode_error();
}

void traceAndThrowDsa(const char* type) {
	auto te = TraceEvent(SevWarnAlways, type);
	te.suppressFor(10);
	if (auto err = ::ERR_get_error()) {
		char buf[256]{
			0,
		};
		::ERR_error_string_n(err, buf, sizeof(buf));
		te.detail("OpenSSLError", static_cast<const char*>(buf));
	}
	throw digital_signature_ops_error();
}

inline PKeyAlgorithm getPKeyAlgorithm(const EVP_PKEY* key) noexcept {
	auto id = ::EVP_PKEY_base_id(key);
	if (id == EVP_PKEY_RSA)
		return PKeyAlgorithm::RSA;
	else if (id == EVP_PKEY_EC)
		return PKeyAlgorithm::EC;
	else
		return PKeyAlgorithm::UNSUPPORTED;
}

} // anonymous namespace

std::string_view pkeyAlgorithmName(PKeyAlgorithm alg) noexcept {
	switch (alg) {
	case PKeyAlgorithm::EC:
		return "EC";
	case PKeyAlgorithm::RSA:
		return "RSA";
	default:
		return "UNSUPPORTED";
	}
}

StringRef doWritePublicKeyPem(Arena& arena, EVP_PKEY* key) {
	ASSERT(key);
	auto mem = AutoCPointer(::BIO_new(::BIO_s_mem()), &::BIO_free);
	if (!mem)
		traceAndThrowEncode("PublicKeyPemWriteInitError");
	if (1 != ::PEM_write_bio_PUBKEY(mem, key))
		traceAndThrowEncode("PublicKeyPemWrite");
	auto bioBuf = std::add_pointer_t<char>{};
	auto const len = ::BIO_get_mem_data(mem, &bioBuf);
	ASSERT_GT(len, 0);
	auto buf = new (arena) uint8_t[len];
	::memcpy(buf, bioBuf, len);
	return StringRef(buf, static_cast<int>(len));
}

StringRef doWritePublicKeyDer(Arena& arena, EVP_PKEY* key) {
	auto len = 0;
	len = ::i2d_PUBKEY(key, nullptr);
	ASSERT_LT(0, len);
	auto buf = new (arena) uint8_t[len];
	auto out = std::add_pointer_t<uint8_t>(buf);
	len = ::i2d_PUBKEY(key, &out);
	return StringRef(buf, len);
}

bool doVerifyStringSignature(StringRef data, StringRef signature, const EVP_MD& digest, EVP_PKEY* key) {
	auto mdctx = AutoCPointer(::EVP_MD_CTX_create(), &::EVP_MD_CTX_free);
	if (!mdctx) {
		traceAndThrowDsa("PKeyVerifyInitFail");
	}
	if (1 != ::EVP_DigestVerifyInit(mdctx, nullptr, &digest, nullptr, key)) {
		traceAndThrowDsa("PKeyVerifyInitFail");
	}
	if (1 != ::EVP_DigestVerifyUpdate(mdctx, data.begin(), data.size())) {
		traceAndThrowDsa("PKeyVerifyUpdateFail");
	}
	if (1 != ::EVP_DigestVerifyFinal(mdctx, signature.begin(), signature.size())) {
		return false;
	}
	return true;
}

PublicKey::PublicKey(PemEncoded, StringRef pem) {
	ASSERT(!pem.empty());
	auto mem = AutoCPointer(::BIO_new_mem_buf(pem.begin(), pem.size()), &::BIO_free);
	if (!mem)
		traceAndThrowDecode("PemMemBioInitError");
	auto key = ::PEM_read_bio_PUBKEY(mem, nullptr, nullptr, nullptr);
	if (!key)
		traceAndThrowDecode("PemReadPublicKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
	if (algorithm() == PKeyAlgorithm::UNSUPPORTED) {
		TraceEvent(SevWarnAlways, "UnsupportedPKeyAlgorithm")
		    .suppressFor(10)
		    .detail("Algorithm", ::OBJ_nid2sn(EVP_PKEY_base_id(ptr.get())));
		throw pkey_decode_error();
	}
}

PublicKey::PublicKey(DerEncoded, StringRef der) {
	ASSERT(!der.empty());
	auto data = der.begin();
	auto key = ::d2i_PUBKEY(nullptr, &data, der.size());
	if (!key)
		traceAndThrowDecode("DerReadPublicKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
	if (algorithm() == PKeyAlgorithm::UNSUPPORTED) {
		TraceEvent(SevWarnAlways, "UnsupportedPKeyAlgorithm")
		    .suppressFor(10)
		    .detail("Algorithm", ::OBJ_nid2sn(EVP_PKEY_base_id(ptr.get())));
		throw pkey_decode_error();
	}
}

StringRef PublicKey::writePem(Arena& arena) const {
	return doWritePublicKeyPem(arena, nativeHandle());
}

StringRef PublicKey::writeDer(Arena& arena) const {
	return doWritePublicKeyDer(arena, nativeHandle());
}

PKeyAlgorithm PublicKey::algorithm() const {
	auto key = nativeHandle();
	ASSERT(key);
	return getPKeyAlgorithm(key);
}

std::string_view PublicKey::algorithmName() const {
	return pkeyAlgorithmName(this->algorithm());
}

bool PublicKey::verify(StringRef data, StringRef signature, const EVP_MD& digest) const {
	return doVerifyStringSignature(data, signature, digest, nativeHandle());
}

PrivateKey::PrivateKey(PemEncoded, StringRef pem) {
	ASSERT(!pem.empty());
	auto mem = AutoCPointer(::BIO_new_mem_buf(pem.begin(), pem.size()), &::BIO_free);
	if (!mem)
		traceAndThrowDecode("PrivateKeyDecodeInitError");
	auto key = ::PEM_read_bio_PrivateKey(mem, nullptr, nullptr, nullptr);
	if (!key)
		traceAndThrowDecode("PemReadPrivateKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
	if (algorithm() == PKeyAlgorithm::UNSUPPORTED) {
		TraceEvent(SevWarnAlways, "UnsupportedPKeyAlgorithm")
		    .suppressFor(10)
		    .detail("Algorithm", ::OBJ_nid2sn(EVP_PKEY_base_id(ptr.get())));
		throw pkey_decode_error();
	}
}

PrivateKey::PrivateKey(DerEncoded, StringRef der) {
	ASSERT(!der.empty());
	auto data = der.begin();
	auto key = ::d2i_AutoPrivateKey(nullptr, &data, der.size());
	if (!key)
		traceAndThrowDecode("DerReadPrivateKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
	if (algorithm() == PKeyAlgorithm::UNSUPPORTED) {
		TraceEvent(SevWarnAlways, "UnsupportedPKeyAlgorithm")
		    .suppressFor(10)
		    .detail("Algorithm", ::OBJ_nid2sn(EVP_PKEY_base_id(ptr.get())));
		throw pkey_decode_error();
	}
}

StringRef PrivateKey::writePem(Arena& arena) const {
	ASSERT(ptr);
	auto mem = AutoCPointer(::BIO_new(::BIO_s_mem()), &::BIO_free);
	if (!mem)
		traceAndThrowEncode("PrivateKeyPemWriteInitError");
	if (1 != ::PEM_write_bio_PrivateKey(mem, nativeHandle(), nullptr, nullptr, 0, 0, nullptr))
		traceAndThrowEncode("PrivateKeyDerPemWrite");
	auto bioBuf = std::add_pointer_t<char>{};
	auto const len = ::BIO_get_mem_data(mem, &bioBuf);
	ASSERT_GT(len, 0);
	auto buf = new (arena) uint8_t[len];
	::memcpy(buf, bioBuf, len);
	return StringRef(buf, static_cast<int>(len));
}

StringRef PrivateKey::writeDer(Arena& arena) const {
	ASSERT(ptr);
	auto len = 0;
	len = ::i2d_PrivateKey(nativeHandle(), nullptr);
	ASSERT_LT(0, len);
	auto buf = new (arena) uint8_t[len];
	auto out = std::add_pointer_t<uint8_t>(buf);
	len = ::i2d_PrivateKey(nativeHandle(), &out);
	return StringRef(buf, len);
}

StringRef PrivateKey::writePublicKeyPem(Arena& arena) const {
	return doWritePublicKeyPem(arena, nativeHandle());
}

StringRef PrivateKey::writePublicKeyDer(Arena& arena) const {
	return doWritePublicKeyDer(arena, nativeHandle());
}

PKeyAlgorithm PrivateKey::algorithm() const {
	auto key = nativeHandle();
	ASSERT(key);
	return getPKeyAlgorithm(key);
}

std::string_view PrivateKey::algorithmName() const {
	return pkeyAlgorithmName(this->algorithm());
}

StringRef PrivateKey::sign(Arena& arena, StringRef data, const EVP_MD& digest) const {
	auto key = nativeHandle();
	ASSERT(key);
	auto mdctx = AutoCPointer(::EVP_MD_CTX_create(), &::EVP_MD_CTX_free);
	if (!mdctx)
		traceAndThrowDsa("PKeySignInitError");
	if (1 != ::EVP_DigestSignInit(mdctx, nullptr, &digest, nullptr, nativeHandle()))
		traceAndThrowDsa("PKeySignInitError");
	if (1 != ::EVP_DigestSignUpdate(mdctx, data.begin(), data.size()))
		traceAndThrowDsa("PKeySignUpdateError");
	auto sigLen = size_t{};
	if (1 != ::EVP_DigestSignFinal(mdctx, nullptr, &sigLen)) // assess the length first
		traceAndThrowDsa("PKeySignFinalGetLengthError");
	auto sigBuf = new (arena) uint8_t[sigLen];
	if (1 != ::EVP_DigestSignFinal(mdctx, sigBuf, &sigLen))
		traceAndThrowDsa("SignTokenFinalError");
	return StringRef(sigBuf, sigLen);
}

bool PrivateKey::verify(StringRef data, StringRef signature, const EVP_MD& digest) const {
	return doVerifyStringSignature(data, signature, digest, nativeHandle());
}

PublicKey PrivateKey::toPublic() const {
	auto arena = Arena();
	return PublicKey(DerEncoded{}, writePublicKeyDer(arena));
}
