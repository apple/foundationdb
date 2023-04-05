/*
 * Base64Encode.cpp
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

#include "fdbrpc/Base64Encode.h"

namespace {

// work around GCC bug 87476 (~9.0)
static const uint8_t urlEncodedTable[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
static const uint8_t regularBase64Table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

template <bool UrlEncode>
uint8_t encodeValue(uint8_t valueIn) noexcept {
	if constexpr (UrlEncode) {
		return urlEncodedTable[valueIn];
	} else {
		return regularBase64Table[valueIn];
	}
}

template <bool UrlEncode>
int doEncode(const uint8_t* __restrict plaintextIn, int lengthIn, uint8_t* __restrict codeOut) noexcept {
	const uint8_t* plainchar = plaintextIn;
	const uint8_t* const plaintextEnd = plaintextIn + lengthIn;
	uint8_t* codechar = codeOut;
	uint8_t result = 0;
	uint8_t fragment = 0;

	while (1) {
		if (plainchar == plaintextEnd) {
			return codechar - codeOut;
		}
		// byte 1 of 3
		fragment = *plainchar++;
		result = (fragment & 0x0fc) >> 2;
		*codechar++ = encodeValue<UrlEncode>(result);
		result = (fragment & 0x003) << 4;
		if (plainchar == plaintextEnd) {
			*codechar++ = encodeValue<UrlEncode>(result);
			if constexpr (!UrlEncode) {
				*codechar++ = '=';
				*codechar++ = '=';
			}
			return codechar - codeOut;
		}
		// byte 2 of 3
		fragment = *plainchar++;
		result |= (fragment & 0x0f0) >> 4;
		*codechar++ = encodeValue<UrlEncode>(result);
		result = (fragment & 0x00f) << 2;
		if (plainchar == plaintextEnd) {
			*codechar++ = encodeValue<UrlEncode>(result);
			if constexpr (!UrlEncode) {
				*codechar++ = '=';
			}
			return codechar - codeOut;
		}
		// byte 3 of 3
		fragment = *plainchar++;
		result |= (fragment & 0x0c0) >> 6;
		*codechar++ = encodeValue<UrlEncode>(result);
		result = (fragment & 0x03f) >> 0;
		*codechar++ = encodeValue<UrlEncode>(result);
	}
	/* control should not reach here */
	return codechar - codeOut;
}

template <bool UrlEncode>
int getEncodedLength(int dataLength) noexcept {
	if constexpr (UrlEncode) {
		auto r = dataLength % 3;
		if (r == 0)
			return (dataLength / 3) * 4;
		else
			return (dataLength / 3) * 4 + r + 1;
	} else {
		// any non-zero remainder after dividing the input length by 3 results in 4 extra output chars due to padding
		return ((dataLength + 2) / 3) * 4;
	}
}

template <bool UrlEncode>
StringRef doEncodeWithArena(Arena& arena, StringRef plainText) {
	auto encodedLen = getEncodedLength<UrlEncode>(plainText.size());
	if (encodedLen <= 0)
		return StringRef();
	auto out = new (arena) uint8_t[encodedLen];
	auto actualLen = doEncode<UrlEncode>(plainText.begin(), plainText.size(), out);
	ASSERT_EQ(encodedLen, actualLen);
	return StringRef(out, encodedLen);
}

} // anonymous namespace

namespace base64 {

int encode(const uint8_t* __restrict plaintextIn, int lengthIn, uint8_t* __restrict codeOut) noexcept {
	return doEncode<false>(plaintextIn, lengthIn, codeOut);
}

int encodedLength(int dataLength) noexcept {
	return getEncodedLength<false>(dataLength);
}

StringRef encode(Arena& arena, StringRef plainText) {
	return doEncodeWithArena<false>(arena, plainText);
}

namespace url {

int encode(const uint8_t* __restrict plaintextIn, int lengthIn, uint8_t* __restrict codeOut) noexcept {
	return doEncode<true>(plaintextIn, lengthIn, codeOut);
}

int encodedLength(int dataLength) noexcept {
	return getEncodedLength<true>(dataLength);
}

StringRef encode(Arena& arena, StringRef plainText) {
	return doEncodeWithArena<true>(arena, plainText);
}

} // namespace url

} // namespace base64
