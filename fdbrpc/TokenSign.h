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

struct AuthTokenRef {
	static constexpr FileIdentifier file_identifier = 1523118;
	double expiresAt;
	IPAddress ipAddress;
	VectorRef<StringRef> tenants;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, expiresAt, ipAddress, tenants);
	}
};

struct SignedAuthTokenRef {
	static constexpr FileIdentifier file_identifier = 5916732;
	StringRef token;
	StringRef keyName;
	StringRef signature;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, token, keyName, signature);
	}
};

Standalone<SignedAuthTokenRef> signToken(AuthTokenRef token, StringRef keyName, StringRef privateKeyDer);

bool verifyToken(SignedAuthTokenRef signedToken, StringRef publicKeyDer);

#endif // FDBRPC_TOKEN_SIGN_H
