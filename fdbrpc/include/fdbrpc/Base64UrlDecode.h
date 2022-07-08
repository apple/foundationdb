/*
 * Base64UrlDecode.h
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

#ifndef BASE64_URLDECODE_H
#define BASE64_URLDECODE_H

#include <cstdint>
#include <utility>
#include "flow/Arena.h"

namespace base64url {

// libb64 (https://github.com/libb64/libb64) adapted for decoding url-encoded base64.
// Key differences from libb64's base64 decoding functions:
// 1. Replace '+' with '-' and '/' with '_'
// 2. Expect no '=' padding at the end
// 3. Illegal sequence or character leads to return code -1, not silently skipped.
// 4. One-off decode: assumes one continuous string, no blocks.

// Returns the length of produced plaintext data if code is valid, -1 otherwise.
int decode(const uint8_t* __restrict codeIn, const int lengthIn, uint8_t* __restrict plaintextOut) noexcept;

// Assuming correctly url-encoded base64, get the decoded length
// Returns -1 for invalid length (4n-3)
int decodedLength(int codeLength) noexcept;

// return, if base64UrlStr is valid, a StringRef containing a valid decoded string
// Note: even if decoding fails by bad encoding, StringRef memory still stays allocated from arena
Optional<StringRef> decode(Arena& arena, StringRef base64UrlStr);

} // namespace base64url

#endif /* BASE64_URLDECODE_H */
