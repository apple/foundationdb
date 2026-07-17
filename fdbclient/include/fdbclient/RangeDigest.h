/*
 * RangeDigest.h
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

#ifndef FDBCLIENT_RANGEDIGEST_H
#define FDBCLIENT_RANGEDIGEST_H
#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "fdbclient/FDBTypes.h"

// RangeDigest is a content fingerprint over a set of key-value pairs, designed
// so that storage servers can each hash the data they own locally and the
// per-range results can be combined into a single cluster-wide root over the
// network without moving any user data.
//
// Construction (an incremental multiset / "AdHash" hash):
//   - Per key-value pair, the canonical leaf encoding is, byte-for-byte:
//         uint32 big-endian len(key) | key | uint32 big-endian len(value) | value
//     and leaf = SHA-256(that encoding). This leaf encoding is identical to the
//     Phase-0 external fingerprint tool (fdbfingerprint) so the two agree on the
//     per-key-value hash.
//   - The digest of a range is the sum, modulo 2^256, of the leaf hashes of the
//     key-values in that range. Interpreting the 32-byte state as a big-endian
//     256-bit integer, `combine` is modular addition.
//
// Because addition is associative and commutative, the digest of a range equals
// the sum of the digests of any partition of that range, regardless of how the
// data is physically sharded. This is what makes before-backup vs after-restore
// roots comparable even though shard boundaries move: the root depends only on
// the multiset of key-value pairs (and keys are unique), not on the partition.
//
// A mismatch of two roots can be localized by comparing the persisted per-range
// digests and, because the combine is additive, by re-running a digest over a
// narrower key range to bisect the divergent region.
struct RangeDigest {
	// Big-endian 256-bit accumulator (most-significant byte first). Zero is the
	// identity for combine and the digest of the empty set.
	std::array<uint8_t, 32> state{};

	RangeDigest() = default;

	// Fold one key-value pair into the accumulator.
	void addKeyValue(StringRef key, StringRef value);

	// this = (this + other) mod 2^256.
	void combine(const RangeDigest& other);

	// Raw 32-byte big-endian state, for serialization/persistence.
	std::string bytes() const { return std::string(reinterpret_cast<const char*>(state.data()), state.size()); }

	// Lower-case hex of the 32-byte state (64 chars).
	std::string toHex() const;

	// Parse a 32-byte raw state produced by bytes(). An empty or wrong-length
	// input yields the zero digest (treated as "no contribution").
	static RangeDigest fromBytes(StringRef raw);

	bool operator==(const RangeDigest& o) const { return state == o.state; }
	bool operator!=(const RangeDigest& o) const { return state != o.state; }
	bool isZero() const;
};

#endif
