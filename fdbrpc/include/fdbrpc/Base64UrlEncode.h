/*
 * Base64UrlEncode.h
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

#ifndef BASE64_URLENCODE_H
#define BASE64_URLENCODE_H

#include <cstdint>
#include "flow/Arena.h"

namespace base64url {

// libb64 (https://github.com/libb64/libb64) adapted for url-encoded base64
// Key differences from libb64's base64 encoding functions:
// 1. Replace '+' with '-' and '/' with '_'
// 2. No '=' padding at the end
// 3. No line wrap every 72 chars
// 4. One-off encode: assumes one continuous string, no streaming.

int encode(const uint8_t* __restrict plaintextIn, int lengthIn, uint8_t* __restrict codeOut) noexcept;

// Return the number of bytes required to store the data of given length in b64url encoding.
int encodedLength(int dataLength) noexcept;

// encode passed string into memory allocated from arena
StringRef encode(Arena& arena, StringRef plainText);

} // namespace base64url

#endif /* BASE64_URLENCODE_H */
