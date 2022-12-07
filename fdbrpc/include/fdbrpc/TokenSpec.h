/*
 * TokenSpec.h
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

#ifndef FDBRPC_TOKEN_SPEC_H
#define FDBRPC_TOKEN_SPEC_H
#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace authz {

enum class Algorithm : int {
	RS256,
	ES256,
	UNKNOWN,
};

inline Algorithm algorithmFromString(std::string_view s) noexcept {
	if (s == "RS256")
		return Algorithm::RS256;
	else if (s == "ES256")
		return Algorithm::ES256;
	else
		return Algorithm::UNKNOWN;
}

} // namespace authz

namespace authz::jwt {

// Given S = concat(B64UrlEnc(headerJson), ".", B64UrlEnc(payloadJson)),
// JWT is concat(S, ".", B64UrlEnc(sign(S, PrivateKey))).
// Below we refer to S as "sign input"

// This struct is not meant to be flatbuffer-serialized
// This is a parsed, flattened view of S and signature

template <class StringType, template <class...> class VectorType, template <class> class OptionalType = std::optional>
struct BasicTokenSpec {
	// header part ("typ": "JWT" implicitly enforced)
	Algorithm algorithm; // alg
	StringType keyId; // kid
	// payload part
	OptionalType<StringType> issuer; // iss
	OptionalType<StringType> subject; // sub
	OptionalType<VectorType<StringType>> audience; // aud
	OptionalType<uint64_t> issuedAtUnixTime; // iat
	OptionalType<uint64_t> expiresAtUnixTime; // exp
	OptionalType<uint64_t> notBeforeUnixTime; // nbf
	OptionalType<StringType> tokenId; // jti
	OptionalType<VectorType<StringType>> tenants; // tenants
	// signature part
	StringType signature;
};

} // namespace authz::jwt

#endif /*FDBRPC_TOKEN_SPEC_H*/
