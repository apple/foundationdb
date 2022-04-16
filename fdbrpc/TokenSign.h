/*
 * TokenSign.h
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

#pragma once
#ifndef FDBRPC_TOKEN_SIGN_H
#define FDBRPC_TOKEN_SIGN_H

#include "flow/network.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/FileIdentifier.h"
#include "fdbrpc/TenantInfo.h"
#include "fdbrpc/TenantInfo.h"

struct AuthTokenRef {
	static constexpr FileIdentifier file_identifier = 1523118;
	double expiresAt;
	VectorRef<TenantNameRef> tenants;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, expiresAt, tenants);
	}
};

struct SignedAuthTokenRef {
	static constexpr FileIdentifier file_identifier = 5916732;
	SignedAuthTokenRef() {}
	SignedAuthTokenRef(Arena& p, const SignedAuthTokenRef& other)
	  : token(p, other.token), keyName(p, other.keyName), signature(p, other.signature) {}

	StringRef token;
	StringRef keyName;
	StringRef signature;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, token, keyName, signature);
	}

	int expectedSize() const { return token.size() + keyName.size() + signature.size(); }
};

using SignedAuthToken = Standalone<SignedAuthTokenRef>;

SignedAuthToken signToken(AuthTokenRef token, StringRef keyName, StringRef privateKeyDer);

bool verifyToken(SignedAuthTokenRef signedToken, StringRef publicKeyDer);

// Below method is intented to be used for testing only

struct KeyPairRef {
	StringRef privateKey;
	StringRef publicKey;
};

Standalone<KeyPairRef> generateEcdsaKeyPair();

#endif // FDBRPC_TOKEN_SIGN_H
