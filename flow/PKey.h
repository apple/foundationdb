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

#ifndef FLOW_PKEY_H
#define FLOW_PKEY_H

#include <memory>
#include <openssl/evp.h>
#include "flow/Arena.h"

struct PemEncoded {};
struct DerEncoded {};

class PrivateKey;

// Consumes public key in ASN.1 subjectPublicKeyInfo encoding
class PublicKey {
	std::shared_ptr<EVP_PKEY> ptr;

public:
	PublicKey() noexcept = default;

	// PEM_read_bio_PUBKEY
	PublicKey(PemEncoded, StringRef pem);

	// d2i_PUBKEY
	PublicKey(DerEncoded, StringRef der);

	PublicKey(const PublicKey& other) noexcept = default;

	PublicKey& operator=(const PublicKey& other) noexcept = default;

	// PEM_write_bio_PUBKEY
	StringRef writePem(Arena& arena) const;

	// i2d_PUBKEY
	StringRef writeDer(Arena& arena) const;

	EVP_PKEY* nativeHandle() const noexcept { return ptr.get(); }

	explicit operator bool() const noexcept { return static_cast<bool>(ptr); }
};

class PrivateKey {
	std::shared_ptr<EVP_PKEY> ptr;

public:
	PrivateKey() noexcept = default;

	// PEM_read_bio_PrivateKey
	PrivateKey(PemEncoded, StringRef pem);

	// d2i_AutoPrivateKey
	PrivateKey(DerEncoded, StringRef der);

	// Unsafe. Use when you're sure of unsafePtr's content & lifetime
	PrivateKey(std::shared_ptr<EVP_PKEY> unsafePtr);

	PrivateKey(const PrivateKey& other) noexcept = default;

	PrivateKey& operator=(const PrivateKey& other) noexcept = default;

	// PEM_write_bio_PrivateKey
	StringRef writePem(Arena& arena) const;

	// d2i_PrivateKey
	StringRef writeDer(Arena& arena) const;

	// PEM_write_bio_PUBKEY
	StringRef writePublicKeyPem(Arena& arena) const;

	// i2d_PUBKEY
	StringRef writePublicKeyDer(Arena& arena) const;

	EVP_PKEY* nativeHandle() const noexcept { return ptr.get(); }

	explicit operator bool() const noexcept { return static_cast<bool>(ptr); }

	// Create a PublicKey independent of this key
	PublicKey toPublicKey() const;
};
#endif /*FLOW_PKEY_H*/
