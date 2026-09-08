/*
 * RangeDigest.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/RangeDigest.h"

#include <cstring>
#include <openssl/sha.h>

namespace {

// Write a big-endian uint32 length prefix into the SHA-256 context.
void updateLengthPrefix(SHA256_CTX& ctx, uint32_t n) {
	uint8_t lb[4];
	lb[0] = static_cast<uint8_t>(n >> 24);
	lb[1] = static_cast<uint8_t>(n >> 16);
	lb[2] = static_cast<uint8_t>(n >> 8);
	lb[3] = static_cast<uint8_t>(n);
	SHA256_Update(&ctx, lb, sizeof(lb));
}

} // namespace

void RangeDigest::addKeyValue(StringRef key, StringRef value) {
	// leaf = SHA-256( u32be len(key) | key | u32be len(value) | value )
	SHA256_CTX ctx;
	SHA256_Init(&ctx);
	updateLengthPrefix(ctx, static_cast<uint32_t>(key.size()));
	SHA256_Update(&ctx, key.begin(), key.size());
	updateLengthPrefix(ctx, static_cast<uint32_t>(value.size()));
	SHA256_Update(&ctx, value.begin(), value.size());
	uint8_t leaf[SHA256_DIGEST_LENGTH];
	SHA256_Final(leaf, &ctx);

	static_assert(SHA256_DIGEST_LENGTH == 32, "RangeDigest state is 256 bits");

	// state = (state + leaf) mod 2^256, big-endian with carry from LSB (byte 31).
	uint16_t carry = 0;
	for (int i = 31; i >= 0; --i) {
		uint16_t sum = static_cast<uint16_t>(state[i]) + static_cast<uint16_t>(leaf[i]) + carry;
		state[i] = static_cast<uint8_t>(sum & 0xff);
		carry = sum >> 8;
	}
}

void RangeDigest::combine(const RangeDigest& other) {
	uint16_t carry = 0;
	for (int i = 31; i >= 0; --i) {
		uint16_t sum = static_cast<uint16_t>(state[i]) + static_cast<uint16_t>(other.state[i]) + carry;
		state[i] = static_cast<uint8_t>(sum & 0xff);
		carry = sum >> 8;
	}
}

std::string RangeDigest::toHex() const {
	static const char* hexDigits = "0123456789abcdef";
	std::string out;
	out.reserve(64);
	for (uint8_t b : state) {
		out.push_back(hexDigits[b >> 4]);
		out.push_back(hexDigits[b & 0xf]);
	}
	return out;
}

RangeDigest RangeDigest::fromBytes(StringRef raw) {
	RangeDigest d;
	if (raw.size() == d.state.size()) {
		memcpy(d.state.data(), raw.begin(), d.state.size());
	}
	return d;
}

bool RangeDigest::isZero() const {
	for (uint8_t b : state) {
		if (b != 0) {
			return false;
		}
	}
	return true;
}
