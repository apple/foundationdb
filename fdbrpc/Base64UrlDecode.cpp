/*
 * Base64UrlDecode.cpp
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

#include "fdbrpc/Base64UrlDecode.h"
#include "flow/Error.h"

namespace base64url {

constexpr uint8_t _X = 0xff;

inline uint8_t decodeValue(uint8_t valueIn) noexcept {
	constexpr const uint8_t decoding[] = { // 20x13
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, 62, _X, _X, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, _X, _X,
		_X, _X, _X, _X, _X,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
		15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, _X, _X, _X, _X, 63, _X, 26, 27, 28,
		29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
		49, 50, 51, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
		_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X };
	static_assert(sizeof(decoding) / sizeof(decoding[0]) == 256);
	return decoding[valueIn];
}

int decode(const uint8_t* __restrict codeIn, const int lengthIn, uint8_t* __restrict plaintextOut) noexcept {
	const uint8_t* codechar = codeIn;
	const uint8_t* const codeEnd = codeIn + lengthIn;
	uint8_t* plainchar = plaintextOut;
	uint8_t fragment = 0;

	while (1) {
		// code 1 of 4
		if (codechar == codeEnd) {
			return plainchar - plaintextOut;
		}
		fragment = decodeValue(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar = (fragment & 0x03f) << 2;
		if (codechar == codeEnd) {
			return -1; // requires at least 2 chars to decode 1 plain byte
		}
		// code 2 of 4
		fragment = decodeValue(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar++ |= (fragment & 0x030) >> 4;
		if (codechar == codeEnd) {
			return plainchar - plaintextOut;
		}
		*plainchar = (fragment & 0x00f) << 4;
		// code 3 of 4
		fragment = decodeValue(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar++ |= (fragment >> 2);
		if (codechar == codeEnd) {
			return plainchar - plaintextOut;
		}
		*plainchar = (fragment & 0x003) << 6;
		// code 4 of 4
		fragment = decodeValue(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar++ |= (fragment & 0x03f);
	}
	/* control should not reach here */
	return plainchar - plaintextOut;
}

int decodedLength(int codeLength) noexcept {
	const auto r = (codeLength & 3);
	if (r == 1) return -1;
	else if (r == 0) return (codeLength / 4) * 3;
	else return (codeLength / 4) * 3 + (r - 1);
}

std::pair<StringRef, bool> decode(Arena& arena, StringRef base64UrlStr) {
	auto decodedLen = decodedLength(base64UrlStr.size());
	if (decodedLen <= 0) {
		return {StringRef(), decodedLen == 0};
	}
	auto out = new (arena) uint8_t[decodedLen];
	auto actualLen = decode(base64UrlStr.begin(), base64UrlStr.size(), out);
	ASSERT_EQ(decodedLen, actualLen);
	return {StringRef(out, decodedLen), true};
}

} // namespace base64url
