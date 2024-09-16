/*
 * JsonWebKeySet.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBRPC_JSON_WEB_KEY_SET_H
#define FDBRPC_JSON_WEB_KEY_SET_H
#include "flow/Arena.h"
#include "flow/PKey.h"
#include <map>
#include <variant>

struct PublicOrPrivateKey {
	std::variant<PublicKey, PrivateKey> key;

	PublicOrPrivateKey() noexcept = default;

	PublicOrPrivateKey(PublicKey key) noexcept : key(std::in_place_type<PublicKey>, key) {}

	PublicOrPrivateKey(PrivateKey key) noexcept : key(std::in_place_type<PrivateKey>, key) {}

	bool isPublic() const noexcept { return std::holds_alternative<PublicKey>(key); }

	bool isPrivate() const noexcept { return std::holds_alternative<PrivateKey>(key); }

	PublicKey getPublic() const { return std::get<PublicKey>(key); }

	PrivateKey getPrivate() const { return std::get<PrivateKey>(key); }
};

// Implements JWKS standard in restricted scope:
// - Parses and stores public/private keys (but not shared keys) as OpenSSL internal types
// - Accept only a) EC algorithm with P-256 curve or b) RSA algorithm.
// - Each key object must meet following requirements:
//   - "alg" field is set to either "ES256" or "RS256"
//   - "kty" field is set to "EC" or "RSA"
//   - "kty" field matches the "alg" field: i.e. EC for "alg":"ES256" and RSA for "alg":"RS256"
struct JsonWebKeySet {
	using KeyMap = std::map<Standalone<StringRef>, PublicOrPrivateKey>;
	KeyMap keys;

	// Parse JWKS string to map of KeyName-PKey
	// If allowedUses is not empty, JWK's optional "use" member is verified against it.
	// Otherwise, uses are all-inclusive.
	static Optional<JsonWebKeySet> parse(StringRef jwksString, VectorRef<StringRef> allowedUses);

	// Returns JSON string representing the JSON Web Key Set.
	// Inverse operation of parse(). Only allows keys expected/accepted by parse().
	Optional<StringRef> toStringRef(Arena& arena);
};

#endif // FDBRPC_JSON_WEB_KEY_SET_H
