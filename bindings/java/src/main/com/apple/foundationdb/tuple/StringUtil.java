/*
 * StringUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.tuple;

final class StringUtil {
	private static final char SURROGATE_COUNT = Character.MAX_LOW_SURROGATE - Character.MIN_HIGH_SURROGATE + 1;
	private static final char ABOVE_SURROGATES = Character.MAX_VALUE - Character.MAX_LOW_SURROGATE;

	static char adjustForSurrogates(char c, String s, int pos) {
		if(c > Character.MAX_LOW_SURROGATE) {
			return (char)(c - SURROGATE_COUNT);
		}
		else {
			// Validate the UTF-16 string as this can do weird things on invalid strings
			if((Character.isHighSurrogate(c) && (pos + 1 >= s.length() || !Character.isLowSurrogate(s.charAt(pos + 1)))) ||
					(Character.isLowSurrogate(c) && (pos == 0 || !Character.isHighSurrogate(s.charAt(pos - 1))))) {
				throw new IllegalArgumentException("malformed UTF-16 string does not follow high surrogate with low surrogate");
			}
			return (char)(c + ABOVE_SURROGATES);

		}
	}

	// Compare two strings based on their UTF-8 code point values. Note that Java stores strings
	// using UTF-16. However, {@link Tuple}s are encoded using UTF-8. Using unsigned byte comparison,
	// UTF-8 strings will sort based on their Unicode codepoints. However, UTF-16 strings <em>almost</em>,
	// but not quite, sort that way. This can be addressed by fixing up surrogates. There are 0x800 surrogate
	// values and about 0x2000 code points above the maximum surrogate value. For anything that is a surrogate,
	// shift it up by 0x2000, and anything that is above the maximum surrogate value, shift it down by 0x800.
	// This makes all surrogates sort after all non-surrogates.
	//
	// See: https://ssl.icu-project.org/docs/papers/utf16_code_point_order.html
	static int compareUtf8(String s1, String s2) {
		// Ignore common prefix at the beginning which will compare equal regardless of encoding
		int pos = 0;
		while(pos < s1.length() && pos < s2.length() && s1.charAt(pos) == s2.charAt(pos)) {
			pos++;
		}
		if(pos >= s1.length() || pos >= s2.length()) {
			// One string is the prefix of another, so return based on length.
			return Integer.compare(s1.length(), s2.length());
		}
		// Compare first different character
		char c1 = s1.charAt(pos);
		char c2 = s2.charAt(pos);
		// Apply "fix up" for surrogates
		if(c1 >= Character.MIN_HIGH_SURROGATE) {
			c1 = adjustForSurrogates(c1, s1, pos);
		}
		if(c2 >= Character.MIN_HIGH_SURROGATE) {
			c2 = adjustForSurrogates(c2, s2, pos);
		}
		return Character.compare(c1, c2);
	}

	static int packedSize(String s) {
		final int strLength = s.length();
		int size = 0;
		int pos = 0;

		while(pos < strLength) {
			char c = s.charAt(pos);
			if(c == '\0') {
				// Null is encoded as \x00\xff
				size += 2;
			}
			else if(c <= 0x7f) {
				// ASCII code point. Only 1 byte.
				size += 1;
			}
			else if(c <= 0x07ff) {
				// 2 byte code point
				size += 2;
			}
			else if(Character.isHighSurrogate(c)) {
				if(pos + 1 < s.length() && Character.isLowSurrogate(s.charAt(pos + 1))) {
					// High surrogate followed by low surrogate means the code point
					// is between U+10000 and U+10FFFF, so it requires 4 bytes.
					size += 4;
					pos += 1;
				}
				else {
					throw new IllegalArgumentException("malformed UTF-16 has high surrogate not followed by low surrogate");
				}
			}
			else if(Character.isLowSurrogate(c)) {
				throw new IllegalArgumentException("malformed UTF-16 has low surrogate without prior high surrogate");
			}
			else {
				// 3 byte code point
				size += 3;
			}
			pos += 1;
		}

		return size;
	}

	private StringUtil() {}
}
