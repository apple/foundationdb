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

#ifndef FDBRPC_TOKEN_SIGN_H
#define FDBRPC_TOKEN_SIGN_H
#pragma once

#include "flow/network.h"
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"
#include "flow/PKey.h"
#include "fdbrpc/TokenSpec.h"
#include <string>
#include <vector>

namespace authz::jwt {

namespace detail {

// work around the fact that VectorRef takes more than one template parameter
template <typename T>
using VectorRefAlias = VectorRef<T>;

} // namespace detail

using TokenRef = BasicTokenSpec<StringRef, detail::VectorRefAlias, Optional>;

// print each non-signature field in non-JSON, human-readable format e.g. for trace
StringRef toStringRef(Arena& arena, const TokenRef& tokenSpec);

StringRef makeSignInput(Arena& arena, const TokenRef& tokenSpec);

// Sign the passed sign input
StringRef signToken(Arena& arena, StringRef signInput, Algorithm algorithm, PrivateKey privateKey);

// One-stop function to make JWT from spec
StringRef signToken(Arena& arena, const TokenRef& tokenSpec, PrivateKey privateKey);

// Parse passed b64url-encoded header part and materialize its contents into tokenOut,
// using memory allocated from arena
// Returns a non-empty optional containing an error message if parsing failed
Optional<StringRef> parseHeaderPart(Arena& arena, TokenRef& tokenOut, StringRef b64urlHeaderIn);

// Parse passed b64url-encoded payload part and materialize its contents into tokenOut,
// using memory allocated from arena
// Returns a non-empty optional containing an error message if parsing failed
Optional<StringRef> parsePayloadPart(Arena& arena, TokenRef& tokenOut, StringRef b64urlPayloadIn);

// Parse passed b64url-encoded signature part and materialize its contents into tokenOut,
// using memory allocated from arena
// Returns a non-empty optional containing an error message if parsing failed
Optional<StringRef> parseSignaturePart(Arena& arena, TokenRef& tokenOut, StringRef b64urlSignatureIn);

// Parses passed token string and materialize its contents into tokenOut,
// using memory allocated from arena
// Returns a non-empty optional containing an error message if parsing failed
Optional<StringRef> parseToken(Arena& arena,
                               StringRef signedTokenIn,
                               TokenRef& parsedTokenOut,
                               StringRef& signInputOut);

// Using the parsed token metadata and sign input, verify that the signature from parsedToken
// is a result of signing sign input with a private key that matches the provided public key
// Returns a tuple containing signature verification result,
// and an optional containing verification error message if any occurred.
// If the latter value is non-empty, the former value should not be used.
// NOTE: This is more efficient than the other overload, as it re-uses
//       the parsed and base64url-decoded token metadata and signature from parseToken() step
std::pair<bool, Optional<StringRef>> verifyToken(StringRef signInput, const TokenRef& parsedToken, PublicKey publicKey);

// Verifies only the signature part of signed token string against its token part, not its content
// Returns a tuple containing signature verification result,
// and an optional containing verification error message if any occurred.
// If the latter value is non-empty, the former value should not be used.
std::pair<bool, Optional<StringRef>> verifyToken(StringRef signedToken, PublicKey publicKey);

} // namespace authz::jwt

#endif // FDBRPC_TOKEN_SIGN_H
