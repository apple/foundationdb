/*
 * Base64Encode.h
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

#ifndef BASE64_ENCODE_H
#define BASE64_ENCODE_H

#include <cstdint>
#include "flow/Arena.h"

namespace base64 {

// libb64 (https://github.com/libb64/libb64) adapted to support both base64 and URL-encoded base64
// URL-encoded base64 differs from the regular base64 in following aspects:
//  - Replaces '+' with '-' and '/' with '_'
//  - No '=' padding at the end
// NOTE: Unlike libb64, this implementation does NOT line wrap base64-encoded output every 72 chars,
//       URL-encoded or otherwise. Also, every encoding/decoding is one-off: i.e. no streaming.

// Encodes plaintext into base64 string and returns the length of encoded output in bytes
int encode(const uint8_t* __restrict plaintextIn, int lengthIn, uint8_t* __restrict codeOut) noexcept;

// Returns the number of bytes required to store the data of given length in base64 encoding.
int encodedLength(int dataLength) noexcept;

// Encodes passed plaintext into memory allocated from arena
StringRef encode(Arena& arena, StringRef plainText);

namespace url {

// Encodes plaintext into URL-encoded base64 string and returns the length of encoded output in bytes
int encode(const uint8_t* __restrict plaintextIn, int lengthIn, uint8_t* __restrict codeOut) noexcept;

// Returns the number of bytes required to store the data of given length in URL-encoded base64
int encodedLength(int dataLength) noexcept;

// encode passed string into memory allocated from arena
StringRef encode(Arena& arena, StringRef plainText);

} // namespace url

} // namespace base64

#endif /* BASE64_ENCODE_H */
