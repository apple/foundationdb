/*
 * Base64Decode.h
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

#ifndef BASE64_DECODE_H
#define BASE64_DECODE_H

#include <cstdint>
#include <utility>
#include "flow/Arena.h"

namespace base64 {

// libb64 (https://github.com/libb64/libb64) adapted to support both base64 and URL-encoded base64
// URL-encoded base64 differs from the regular base64 in following aspects:
//  - Replaces '+' with '-' and '/' with '_'
//  - No '=' padding at the end
// NOTE: Unlike libb64, this implementation does NOT line wrap base64-encoded output every 72 chars,
//       URL-encoded or otherwise. Also, every encoding/decoding is one-off: i.e. no streaming.

// Decodes base64-encoded input and returns the length of produced plaintext data if input is valid, -1 otherwise.
int decode(const uint8_t* __restrict codeIn, const int lengthIn, uint8_t* __restrict plaintextOut) noexcept;

// Assuming correctly encoded base64 code length, get the decoded length
int decodedLength(int codeLength) noexcept;

// Assuming a correct base64 string input, return the decoded plaintext. Returns an empty Optional if invalid.
// Note: even if decoding fails by bad encoding, StringRef memory still stays allocated from arena
Optional<StringRef> decode(Arena& arena, StringRef input);

namespace url {

// Decodes URL-encoded base64 input and returns the length of produced plaintext data if input is valid, -1 otherwise.
int decode(const uint8_t* __restrict codeIn, const int lengthIn, uint8_t* __restrict plaintextOut) noexcept;

// Assuming correctly URL-encoded base64 code length, get the decoded length
// Returns -1 for invalid length (4n-3)
int decodedLength(int codeLength) noexcept;

// Assuming a correct URL-encoded base64 string input, return the decoded plaintext. Returns an empty Optional if
// invalid. Note: even if decoding fails by bad encoding, StringRef memory still stays allocated from arena
Optional<StringRef> decode(Arena& arena, StringRef input);

} // namespace url

} // namespace base64

#endif /* BASE64_DECODE_H */
