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
#include "flow/PKey.h"

namespace authz {

enum class Algorithm : int {
	RS256,
	ES256,
	UNKNOWN,
};

Algorithm algorithmFromString(StringRef s) noexcept;

} // namespace authz

namespace authz::flatbuffers {

struct TokenRef {
	static constexpr FileIdentifier file_identifier = 1523118;
	double expiresAt;
	VectorRef<StringRef> tenants;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, expiresAt, tenants);
	}
};

struct SignedTokenRef {
	static constexpr FileIdentifier file_identifier = 5916732;
	StringRef token;
	StringRef keyName;
	StringRef signature;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, token, keyName, signature);
	}
};

SignedTokenRef signToken(Arena& arena, TokenRef token, StringRef keyName, PrivateKey privateKey);

bool verifyToken(SignedTokenRef signedToken, PublicKey publicKey);

} // namespace authz::flatbuffers

namespace authz::jwt {

// Given T = concat(B64UrlEnc(headerJson), ".", B64UrlEnc(payloadJson)),
// JWT is concat(T, ".", B64UrlEnc(sign(T, PrivateKey))).
// Below we refer to T as "token part"

// This struct is not meant to be flatbuffer-serialized
// This is a parsed, flattened view of T and signature
struct TokenRef {
	// header part ("typ": "JWT" implicitly enforced)
	Algorithm algorithm; // alg
	// payload part
	Optional<StringRef> issuer; // iss
	Optional<StringRef> subject; // sub
	Optional<VectorRef<StringRef>> audience; // aud
	Optional<uint64_t> issuedAtUnixTime; // iat
	Optional<uint64_t> expiresAtUnixTime; // exp
	Optional<uint64_t> notBeforeUnixTime; // nbf
	Optional<StringRef> keyId; // kid
	Optional<StringRef> tokenId; // jti
	Optional<VectorRef<StringRef>> tenants; // tenants
	// signature part
	StringRef signature;
};

// Make plain JSON token string with fields (except signature) from passed spec
StringRef makeTokenPart(Arena& arena, TokenRef tokenSpec);

// Generate plaintext signature of token part
StringRef makePlainSignature(Arena& arena, Algorithm alg, StringRef tokenPart, PrivateKey privateKey);

// One-stop function to make JWT from spec
StringRef signToken(Arena& arena, TokenRef tokenSpec, PrivateKey privateKey);

// Parse passed b64url-encoded header part and materialize its contents into tokenOut,
// using memory allocated from arena
bool parseHeaderPart(TokenRef& tokenOut, StringRef b64urlHeaderIn);

// Parse passed b64url-encoded payload part and materialize its contents into tokenOut,
// using memory allocated from arena
bool parsePayloadPart(Arena& arena, TokenRef& tokenOut, StringRef b64urlPayloadIn);

// Parse passed b64url-encoded signature part and materialize its contents into tokenOut,
// using memory allocated from arena
bool parseSignaturePart(Arena& arena, TokenRef& tokenOut, StringRef b64urlSignatureIn);

// Parse passed token string and materialize its contents into tokenOut,
// using memory allocated from arena
// Return whether the signed token string is well-formed
bool parseToken(Arena& arena, TokenRef& tokenOut, StringRef signedTokenIn);

// Verify only the signature part of signed token string against its token part, not its content
bool verifyToken(StringRef signedToken, PublicKey publicKey);

} // namespace authz::jwt

#endif // FDBRPC_TOKEN_SIGN_H
