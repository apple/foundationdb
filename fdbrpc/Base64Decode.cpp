/*
 * Base64Decode.cpp
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

#include <string_view>
#include <fmt/format.h>
#include "fdbrpc/Base64Encode.h"
#include "fdbrpc/Base64Decode.h"
#include "flow/flow.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"

namespace {

constexpr uint8_t _X = 0xff;

template <bool UrlDecode>
inline uint8_t decodeValue(uint8_t valueIn) noexcept {
	if constexpr (UrlDecode) {
		// clang-format off
		// Decodes base64url-encoding's 64 legal ASCII character byte: i.e. alphanumeric, '-', and '_'
		// into 6-bit fragment, a sequence containing four of which would form 3 original bytes.
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
		// clang-format on
		static_assert(sizeof(decoding) / sizeof(decoding[0]) == 256);
		return decoding[valueIn];
	} else {
		// clang-format off
		// same as url-encoded base64, except that this encoding assumes '+' instead of '-',
		// and '/' instead of '_'.
		constexpr const uint8_t decoding[] = { // 20x13
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, 62, _X, _X, _X, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, _X, _X,
			_X, _X, _X, _X, _X,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
			15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, _X, _X, _X, _X, _X, _X, 26, 27, 28,
			29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
			49, 50, 51, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X,
			_X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X, _X };
		// clang-format on
		static_assert(sizeof(decoding) / sizeof(decoding[0]) == 256);
		return decoding[valueIn];
	}
}

template <bool UrlDecode>
int doDecode(const uint8_t* __restrict codeIn, const int lengthIn, uint8_t* __restrict plaintextOut) noexcept {
	const uint8_t* codechar = codeIn;
	const uint8_t* const codeEnd = codeIn + lengthIn;
	uint8_t* plainchar = plaintextOut;
	uint8_t fragment = 0;

	while (1) {
		// code 1 of 4
		if (codechar == codeEnd) {
			return plainchar - plaintextOut;
		}
		fragment = decodeValue<UrlDecode>(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar = (fragment & 0x03f) << 2;
		if (codechar == codeEnd) {
			return -1; // requires at least 2 chars to decode 1 plain byte
		}
		// code 2 of 4
		fragment = decodeValue<UrlDecode>(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar++ |= (fragment & 0x030) >> 4;
		if (codechar == codeEnd) {
			return plainchar - plaintextOut;
		}
		*plainchar = (fragment & 0x00f) << 4;
		// code 3 of 4
		fragment = decodeValue<UrlDecode>(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar++ |= (fragment >> 2);
		if (codechar == codeEnd) {
			return plainchar - plaintextOut;
		}
		*plainchar = (fragment & 0x003) << 6;
		// code 4 of 4
		fragment = decodeValue<UrlDecode>(*codechar++);
		if (fragment == _X)
			return -1;
		*plainchar++ |= (fragment & 0x03f);
	}
	/* control should not reach here */
	return plainchar - plaintextOut;
}

// assumes codeLength after padding stripped
int getDecodedLength(int codeLength) noexcept {
	const auto r = (codeLength % 4);
	if (r == 1)
		return -1;
	else if (r == 0)
		return (codeLength / 4) * 3;
	else
		return (codeLength / 4) * 3 + (r - 1);
}

template <bool UrlDecode>
Optional<StringRef> decodeStringRef(Arena& arena, StringRef codeText) {
	if constexpr (!UrlDecode) {
		// check length alignment and strip padding, if any
		if (codeText.size() % 4)
			return {};
		if (!codeText.empty() && codeText.back() == '=')
			codeText.popBack();
		if (!codeText.empty() && codeText.back() == '=')
			codeText.popBack();
	}
	auto decodedLen = getDecodedLength(codeText.size());
	if (decodedLen <= 0) {
		if (decodedLen == 0)
			return StringRef{};
		return {};
	}
	auto out = new (arena) uint8_t[decodedLen];
	auto actualLen = doDecode<UrlDecode>(codeText.begin(), codeText.size(), out);
	if (actualLen == -1) {
		return {};
	}
	ASSERT_EQ(decodedLen, actualLen);
	return StringRef(out, decodedLen);
}

} // anonymous namespace

namespace base64 {

int decodedLength(int codeLength) noexcept {
	// Non-urlencoded base64 cannot predict the decoded length on encoded length alone due to padding,
	// which makes this a conservative estimate, not an exact one.
	return (codeLength / 4) * 3;
}

int decode(const uint8_t* __restrict codeIn, const int lengthIn, uint8_t* __restrict plaintextOut) noexcept {
	if (lengthIn % 4) {
		return -1;
	} else {
		int actualLen = lengthIn;
		if (actualLen > 0 && codeIn[actualLen - 1] == '=')
			actualLen--;
		if (actualLen > 0 && codeIn[actualLen - 1] == '=')
			actualLen--;
		return doDecode<false>(codeIn, actualLen, plaintextOut);
	}
}

Optional<StringRef> decode(Arena& arena, StringRef codeText) {
	return decodeStringRef<false>(arena, codeText);
}

namespace url {

int decodedLength(int codeLength) noexcept {
	return getDecodedLength(codeLength);
}

int decode(const uint8_t* __restrict codeIn, const int lengthIn, uint8_t* __restrict plaintextOut) noexcept {
	return doDecode<true>(codeIn, lengthIn, plaintextOut);
}

Optional<StringRef> decode(Arena& arena, StringRef codeText) {
	return decodeStringRef<true>(arena, codeText);
}

} // namespace url

} // namespace base64

// transform the input on-the-fly to build testcase for non-urlencoded cases
StringRef urlEncodedTestData[][2] = {
	{ ""_sr, ""_sr },
	{ "f"_sr, "Zg"_sr },
	{ "fo"_sr, "Zm8"_sr },
	{ "foo"_sr, "Zm9v"_sr },
	{ "foob"_sr, "Zm9vYg"_sr },
	{ "fooba"_sr, "Zm9vYmE"_sr },
	{ "foobar"_sr, "Zm9vYmFy"_sr },
	{ "Q\xc2\x93\x86\x04H\xfd\"r\x9c\xf7\xafW\xd1\x87^"_sr, "UcKThgRI_SJynPevV9GHXg"_sr },
	{ "\x93"_sr, "kw"_sr },
	{ "\xfcpF\xd2\x15\x03\x8a\xcb\\#!\xa1\x95\x18\x13\xfcpoN\rh=\xa5\x05\xe5\x00\xf8<\xc3\x8b'C\x90\xfc\xa0x\x13"
	  "8q\r\xd4\xca\xc9Yjv"_sr,
	  "_HBG0hUDistcIyGhlRgT_HBvTg1oPaUF5QD4PMOLJ0OQ_KB4EzhxDdTKyVlqdg"_sr },
	{ "\xdd\xbb\x91>@\x9d\x88\x01Qb\x97[\xc3Q\xf6Q\\LF\xe2}\xfb\xf0\xe8\x98\xba\x8c\xc7\xc9\x0e$\xe4q\xcf;\xe4"
	  "e\x02"
	  "DA\xa9\x9a\xf0r\xc9\xf0\xd2-\x98"_sr,
	  "3buRPkCdiAFRYpdbw1H2UVxMRuJ9-_DomLqMx8kOJORxzzvkZQJEQama8HLJ8NItmA"_sr },
	{ "2\x7f\x98\xfe\xb4\x05\x18.%.\xd0\x14\xea\x8e+\xa5\xc5\xbd"
	  "F-Lm\x04\x1aQ\xde\x1e\x9c\x12\xe6\x81{\x9dj\xe8\x9cP\xf4\xf7\x8a<\x12"_sr,
	  "Mn-Y_rQFGC4lLtAU6o4rpcW9Ri1MbQQaUd4enBLmgXudauicUPT3ijwS"_sr },
	{ "\xa3"
	  "b\xc9\x13|\x94\xab)}\xf4N\xbc\xb2\xc5$\x15\xed\xb0\x98\xa4v\x8b\x91\xe4M\xb8\xde!\x94"_sr,
	  "o2LJE3yUqyl99E68ssUkFe2wmKR2i5HkTbjeIZQ"_sr },
	{ "\xa0\xe9\xb4=4\xba\xbd\xbd\xaa\xfd\x96\xcb\x03\xd3\xb7\xc9\xb7i7\x18$^\xba\xe5\xb3\x8a\xf4O\xdb"_sr,
	  "oOm0PTS6vb2q_ZbLA9O3ybdpNxgkXrrls4r0T9s"_sr },
	{ "\x10\xf4zscNoE\xfd\xc5\x8d\x16\x82t|y\n\xcf\xe8\x98\xf8)\xcd\xefm\xe2\xe1%\x17\x05T9;Zb\x05\x02\xc7"
	  "B\x8c\xc5\xc5\x95"
	  "8\xf2"_sr,
	  "EPR6c2NOb0X9xY0WgnR8eQrP6Jj4Kc3vbeLhJRcFVDk7WmIFAsdCjMXFlTjy"_sr },
	{ "NG\"hA\xff\\\xf5lD\xf7\x08"
	  "7\xb9\t\x07\xa5\xb9\xac\x0b\x9fT+\xfa"_sr,
	  "TkciaEH_XPVsRPcIN7kJB6W5rAufVCv6"_sr },
	{ "\xbd\xf5\xb0\x8eh1\x1b\xd1\x13q\x88z0*b\x9cNg\x88\x88MBD\x17\xec\xb0yc3\xbb"_sr,
	  "vfWwjmgxG9ETcYh6MCpinE5niIhNQkQX7LB5YzO7"_sr },
	{ "&\xe0> \xfc\xea\x8e\xc1[I\xec\xe8\x03\x15\tc\x9b\x0f"
	  "d\x13"
	  "d\xab\xa5\x16\xa2p\x91\xd5\x11\xf5X\xa7\xbd\xe1\xa1"
	  "B\x8e\xe8\xddn2\xbf\x97"_sr,
	  "JuA-IPzqjsFbSezoAxUJY5sPZBNkq6UWonCR1RH1WKe94aFCjujdbjK_lw"_sr },
	{ "|W,\xa5\xce\x83\xb0\xec\x87\x86\xd0<O\x94\x97"
	  "0F"_sr,
	  "fFcspc6DsOyHhtA8T5SXMEY"_sr },
	{ "\x06\xa7\xf1"_sr, "Bqfx"_sr },
	{ "u\xc4"
	  "7\x8e&\xa7\x90v"_sr,
	  "dcQ3jiankHY"_sr },
	{ "X\xfe\xcd\x1f\xc8\x8f\xe3\xca"
	  "6\x96\x8c\x87\xcd\xbaJ!\xabq\x8c\x97)#\xfb\xda\xb8\xa9\xe9"
	  "a\x0c\xe2\x10\xe9\xe7\x16\x96\xb5"_sr,
	  "WP7NH8iP48o2loyHzbpKIatxjJcpI_vauKnpYQziEOnnFpa1"_sr },
	{ "\xf1\n@\x18\xc3"
	  "F\xc4\xf8\x1c\xa9\xa9\xdb\x15\xcb\xd0V\xe4P\x8b\x8b\xaf\xf2\xfc\xb7\x1d\xa6p\n\xa3\x13,.\x12#"_sr,
	  "8QpAGMNGxPgcqanbFcvQVuRQi4uv8vy3HaZwCqMTLC4SIw"_sr },
	{ "h\x9e\xe0X_\x1c\x04\xe3\xaf\xac\xe3\x18JK/\xe7\xbc"
	  "D\xf5"
	  "B\xfbK(Q\x8c\n\xca\xfc^\xd9\xdb\xb4\xea\xae\xb8"_sr,
	  "aJ7gWF8cBOOvrOMYSksv57xE9UL7SyhRjArK_F7Z27Tqrrg"_sr },
	{ "\x9aX\xb2Q\xf0\x85R`\xc1\xbb\x95\xe7\x10\xe3\xd0x\n"_sr, "mliyUfCFUmDBu5XnEOPQeAo"_sr },
	{ "\xa7\x1f"
	  "0\x90P\xab\x1fwJ\x86\x82Tj\xd0Ob\xa4\xac+\xc9"
	  "4;\x86\\\x8b\xb6"_sr,
	  "px8wkFCrH3dKhoJUatBPYqSsK8k0O4Zci7Y"_sr },
	{ "\x0e"
	  "C\x87u-\x12\x8a\xe4\x11"
	  "F\xb6"
	  "a5\xcds\x1ez?J4\x02?@g\xaa-\xc4\xe0\x80\xe1!\xc0\x1d\x16\x1e"
	  "Em\xa8"
	  "a\xc8\x9d<\xd1"_sr,
	  "DkOHdS0SiuQRRrZhNc1zHno_SjQCP0Bnqi3E4IDhIcAdFh5FbahhyJ080Q"_sr },
	{ "\xb7\xf6\xb6\\\xd3&\xf5"
	  "F\xb1\x86\xd8n%\xb5"
	  "a\x1d^Iv\xbe\x9bO\xcb\xca\xd2"_sr,
	  "t_a2XNMm9UaxhthuJbVhHV5Jdr6bT8vK0g"_sr },
	{ "\xc0+\xea\xb9\x01\xf2\xe8\x8e\xf2\x0ft\x8b\xa0\xa4\x0cq=\xa6"
	  "c\xf1v\x9b\x81\xc7*\xd0\xe8Z\x04\"\t\xe8>\xd5w\xddQI\xc9\xba\x8f"_sr,
	  "wCvquQHy6I7yD3SLoKQMcT2mY_F2m4HHKtDoWgQiCeg-1XfdUUnJuo8"_sr },
	{ "\xac"
	  "5\x8aH\xc9q\xad\xbe\x1f\x80\xed\xe1"_sr,
	  "rDWKSMlxrb4fgO3h"_sr },
	{ "0}\x82\x95\xbb'\xf2\xdf"
	  "dR\x8f\xc2\xac\xb3\xc7\x9f\xc0\xf0"
	  "C3L\xbe"
	  "E\xe5\xf1\xc4% \xec\xe9"_sr,
	  "MH2Clbsn8t9kUo_CrLPHn8DwQzNMvkXl8cQlIOzp"_sr },
	{ "\x8b\xab\xd7\xf9\xa5\xd8H\x1d"_sr, "i6vX-aXYSB0"_sr },
	{ "i!\xdc\xb7~9\x7f\xad\xa0\x9d\x1e\xcc\xedTj\xe3\xe2\x88Q\x1e\xaa\xf9\xc3\xc5\xc5\xcdq\x9e\x07~\x9e\xcb\xf3\xd3\xb2\xec\xe0[m+\x0c\x9c"_sr,
	  "aSHct345f62gnR7M7VRq4-KIUR6q-cPFxc1xngd-nsvz07Ls4FttKwyc"_sr },
	{ "\x01-{.s\xa5qF4\x1f"
	  "a\x11\xe4\x1eN"_sr,
	  "AS17LnOlcUY0H2ER5B5O"_sr },
	{ "\x1c\xab\xce"
	  "e\xfc\xa7"
	  "are\x1f\x9a\xb4\xcdr\xe2v95\x88"_sr,
	  "HKvOZfynYXJlH5q0zXLidjk1iA"_sr },
	{ "\xde\x0e\x16V\x12\x0f\xa4\xaf"
	  "2\xe7k3\xe8\"\x0b\xcb\x80\xa5\x96,\xba\xef\x1c\xe3\xd8\x16"
	  "C1\xccI\x8a]W\xf0\xbf\xaf\x19"
	  "4\xf9\r*<^?8C\x81\xd3(\xc6"_sr,
	  "3g4WVhIPpK8y52sz6CILy4Clliy67xzj2BZDMcxJil1X8L-vGTT5DSo8Xj84Q4HTKMY"_sr },
	{ "\xe1\xe2\x8a"
	  "8v\x1f\xe0|\xccIJ\t"_sr,
	  "4eKKOHYf4HzMSUoJ"_sr },
};

static Void runTest(std::function<StringRef(Arena&, StringRef)> conversionFn,
                    StringRef (&stringEncodeFn)(Arena&, StringRef),
                    Optional<StringRef> (&stringDecodeFn)(Arena&, StringRef),
                    std::function<void(StringRef, int)> encodeOutputCheckFn) {
	const int testSetLen = sizeof(urlEncodedTestData) / sizeof(urlEncodedTestData[0]);
	for (auto i = 0; i < testSetLen; i++) {
		auto tmpArena = Arena();
		auto [decodeOutputExpected, encodeOutputExpected] = urlEncodedTestData[i];
		// optionally convert base64Url-encoded test input to regular base64
		encodeOutputExpected = conversionFn(tmpArena, encodeOutputExpected);
		auto encodeOutput = stringEncodeFn(tmpArena, decodeOutputExpected);
		if (encodeOutput != encodeOutputExpected) {
			fmt::print("Test case {} (encode): expected '{}' got '{}'\n",
			           i + 1,
			           encodeOutputExpected.toHexString(),
			           encodeOutput.toHexString());
			ASSERT(false);
		}
		auto decodeOutput = stringDecodeFn(tmpArena, encodeOutputExpected);
		ASSERT(decodeOutput.present());
		if (decodeOutput.get() != decodeOutputExpected) {
			fmt::print("Test case {} (decode): expected '{}' got '{}'\n",
			           i + 1,
			           decodeOutputExpected.toHexString(),
			           decodeOutput.get().toHexString());
			ASSERT(false);
		}
	}
	auto& rng = *deterministicRandom();
	for (auto i = 0; i < 100; i++) {
		auto tmpArena = Arena();
		auto inputLen = rng.randomInt(1, 300);
		auto inputBuf = new (tmpArena) uint8_t[inputLen];
		for (auto i = 0; i < inputLen; i++)
			inputBuf[i] = rng.randomInt(0, 256);
		auto input = StringRef(inputBuf, inputLen);
		auto output = stringEncodeFn(tmpArena, input);
		encodeOutputCheckFn(output, i);
		auto decodeOutput = stringDecodeFn(tmpArena, output);
		ASSERT(decodeOutput.present());
		if (input != decodeOutput.get()) {
			fmt::print("Dynamic case {} (decode) failed, expected '{}', got '{}'\n",
			           input.toHexString(),
			           decodeOutput.get().toHexString());
			ASSERT(false);
		}
	}
	return Void();
}

TEST_CASE("/fdbrpc/Base64UrlEncode") {
	return runTest([](Arena&, StringRef input) { return input; }, // no op (input already url-encoded)
	               base64::url::encode,
	               base64::url::decode,
	               [](StringRef encodeOutput, int n) {
		               ASSERT_NE(encodeOutput.size() % 4, 1);
		               // verify that output contains only base64url-legal characters
		               for (auto i = 0; i < encodeOutput.size(); ++i) {
			               auto const value = decodeValue<true /*urlencoded*/>(encodeOutput[i]);
			               if (value == _X) {
				               fmt::print(
				                   "Random-generated case {} has illegal encoded output char: {}th byte, value {}\n",
				                   n + 1,
				                   i + 1,
				                   static_cast<int>(value));
				               ASSERT(false);
			               }
		               }
	               });
}

static StringRef transformBase64UrlToBase64(Arena& arena, StringRef input) {
	if (input.empty())
		return StringRef();
	const int len = ((input.size() + 3) / 4) * 4; // ceil_align(input.size(), 4)
	auto output = new (arena) uint8_t[len];
	for (auto i = 0; i < input.size(); i++) {
		if (input[i] == '-') {
			output[i] = '+';
		} else if (input[i] == '_') {
			output[i] = '/';
		} else {
			output[i] = input[i];
		}
	}
	for (auto i = input.size(); i < len; i++)
		output[i] = '=';
	return StringRef(output, len);
}

TEST_CASE("/fdbrpc/Base64Encode") {
	return runTest(transformBase64UrlToBase64, base64::encode, base64::decode, [](StringRef encodeOutput, int n) {
		ASSERT_EQ(encodeOutput.size() % 4, 0);
		if (!encodeOutput.empty() && encodeOutput.back() == '=')
			encodeOutput.popBack();
		if (!encodeOutput.empty() && encodeOutput.back() == '=')
			encodeOutput.popBack();
		for (auto i = 0; i < encodeOutput.size(); ++i) {
			auto const value = decodeValue<false /*urlencoded*/>(encodeOutput[i]);
			if (value == _X) {
				fmt::print("Random-generated case {} has illegal encoded output char: {}th byte, value {}\n",
				           n + 1,
				           i + 1,
				           static_cast<int>(value));
				ASSERT(false);
			}
		}
	});
}
