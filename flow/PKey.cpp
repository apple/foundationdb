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

#include "flow/Error.h"
#include "flow/PKey.h"
#include "flow/ScopeExit.h"
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

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

} // anonymous namespace

StringRef doWritePublicKeyPem(Arena& arena, EVP_PKEY* key) {
	ASSERT(key);
	auto mem = ::BIO_new(::BIO_s_mem());
	if (!mem)
		traceAndThrowEncode("PublicKeyPemWriteInitError");
	auto memGuard = ScopeExit([mem]() { ::BIO_free(mem); });
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

PublicKey::PublicKey(PemEncoded, StringRef pem) {
	ASSERT(!pem.empty());
	auto mem = ::BIO_new_mem_buf(pem.begin(), pem.size());
	if (!mem)
		traceAndThrowDecode("PemMemBioInitError");
	auto bioGuard = ScopeExit([mem]() { ::BIO_free(mem); });
	auto key = ::PEM_read_bio_PUBKEY(mem, nullptr, nullptr, nullptr);
	if (!key)
		traceAndThrowDecode("PemReadPublicKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
}

PublicKey::PublicKey(DerEncoded, StringRef der) {
	ASSERT(!der.empty());
	auto data = der.begin();
	auto key = ::d2i_PUBKEY(nullptr, &data, der.size());
	if (!key)
		traceAndThrowDecode("DerReadPublicKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
}

StringRef PublicKey::writePem(Arena& arena) const {
	return doWritePublicKeyPem(arena, nativeHandle());
}

StringRef PublicKey::writeDer(Arena& arena) const {
	return doWritePublicKeyDer(arena, nativeHandle());
}

PrivateKey::PrivateKey(PemEncoded, StringRef pem) {
	ASSERT(!pem.empty());
	auto mem = ::BIO_new_mem_buf(pem.begin(), pem.size());
	if (!mem)
		traceAndThrowDecode("PrivateKeyDecodeInitError");
	auto bioGuard = ScopeExit([mem]() { ::BIO_free(mem); });
	auto key = ::PEM_read_bio_PrivateKey(mem, nullptr, nullptr, nullptr);
	if (!key)
		traceAndThrowDecode("PemReadPrivateKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
}

PrivateKey::PrivateKey(DerEncoded, StringRef der) {
	ASSERT(!der.empty());
	auto data = der.begin();
	auto key = ::d2i_AutoPrivateKey(nullptr, &data, der.size());
	if (!key)
		traceAndThrowDecode("DerReadPrivateKeyError");
	ptr = std::shared_ptr<EVP_PKEY>(key, &::EVP_PKEY_free);
}

PrivateKey::PrivateKey(std::shared_ptr<EVP_PKEY> key) : ptr(std::move(key)) {}

StringRef PrivateKey::writePem(Arena& arena) const {
	ASSERT(ptr);
	auto mem = ::BIO_new(::BIO_s_mem());
	if (!mem)
		traceAndThrowEncode("PrivateKeyPemWriteInitError");
	auto memGuard = ScopeExit([mem]() { ::BIO_free(mem); });
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

PublicKey PrivateKey::toPublicKey() const {
	auto arena = Arena();
	return PublicKey(DerEncoded{}, writePublicKeyDer(arena));
}
