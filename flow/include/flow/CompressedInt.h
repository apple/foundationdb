/*
 * CompressedInt.h
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

#pragma once
#include <stdint.h>

// A signed compressed integer format that retains ordering in compressed form.
// Format is: [~sign_bit] [unary_len] [value_bits]
//   If the sign bit is 0 then all other bits are inverted to maintain sort order
//   in encoded form.
//
// Examples: 0/1/n = bit  N=byte   bits (w/o sign)   Total encoded value length
//   10nn nnnn                              6 bits   1 byte
//   110n nnnn N{1}                        13 bits   2 bytes
//   1110 nnnn N{2}                        20 bits   3 bytes
//   1111 0nnn N{3}                        27 bits   4 bytes
//   1111 10nn N{4}                        34 bits   5 bytes
//   1111 110n N{5}                        41 bits   6 bytes
//   1111 1110 N{6}                        48 bits   7 bytes
//   1111 1111 0nnn nnnn N{6}              55 bits   8 bytes
//   1111 1111 10nn nnnn N{7}              62 bits   9 bytes
//   1111 1111 110n nnnn N{8}              69 bits  10 bytes
//   1111 1111 1110 nnnn N{9}              76 bits  11 bytes
//   1111 1111 1111 0nnn N{10}             83 bits  12 bytes
//   1111 1111 1111 10nn N{11}             90 bits  13 bytes
//   1111 1111 1111 110n N{12}             97 bits  14 bytes
//   1111 1111 1111 1110 N{13}            104 bits  15 bytes
//   1111 1111 1111 1111 0nnn nnnn N{13}  111 bits  16 bytes
//   1111 1111 1111 1111 10nn nnnn N{14}  118 bits  17 bytes
//   1111 1111 1111 1111 110n nnnn N{15}  125 bits  18 bytes
template <typename IntType>
struct CompressedInt {
	CompressedInt(IntType i = 0) : value(i) {}
	IntType value;
	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.isDeserializing) {
			uint8_t b;
			serializer(ar, b);
			int bytesToRead = 0; // Additional bytes to read after the required first byte
			bool positive = (b & 0x80) != 0; // Sign bit
			if (!positive)
				b = ~b; // Negative, so invert bytes read
			b &= 0x7f; // Clear sign bit

			uint8_t hb = 0x40; // Next header bit to test
			// Scan the unary len bits across multiple bytes if needed
			while (1) {
				if (hb == 0) { // Go to next byte if needed
					serializer(ar, b); // Read byte
					if (!positive)
						b = ~b; // Negative, so invert bytes read

					hb = 0x80; // Reset header test bit position
					--bytesToRead; // Decrement bytes to read since a byte was just read
				}
				if ((b & hb) == 0) // If a 0 is found, found the end of the unary sequence
					break;
				++bytesToRead; // Found a 1 so increment bytes to read
				b &= ~hb; // Clear the bit just tested.
				hb >>= 1; // Shift header test bit to next lowest position
			}

			value = b; // b contains the highest byte of value
			while (bytesToRead-- != 0) {
				serializer(ar, b); // Read byte
				if (!positive)
					b = ~b; // Negative, so invert bytes read
				value <<= 8; // Shift value up to make room for new byte
				value |= b; // OR the byte into place
			}

			if (!positive) // If negative, reverse all bits
				value = ~value;
		} else {
			uint8_t buf[sizeof(IntType) * 2];
			int iv = sizeof(buf); // Index of last written value byte
			bool neg = value < 0; // If value is negative, flip its bits
			IntType v = neg ? ~value : value;

			// Write the value bytes from LSB to the rightmost zero byte to the output buffer
			while (v) {
				buf[--iv] = (uint8_t)v;
				v >>= 8;
			};

			int bitlen = (sizeof(buf) - iv) * 8; // Value bits written so far
			if (bitlen != 0) { // Reduce bit length by leading 0s in highest value byte
				uint8_t b = buf[iv]; // Get highest value byte
				while (!(b & 0x80)) { // While its highest bit is not a 1
					--bitlen; // Decrement bit length
					b <<= 1; // Shift left to test next lowest position
				}
			}

			int encodedLen = bitlen / 7 + 1; // Calculate length of total encoded value
			int iStart = sizeof(buf) - encodedLen; // Starting index of encoded output byte
			for (int ih = iStart; ih < iv; ++ih) // Clear any bytes not initialized with a value bit
				buf[ih] = 0;
			int ih = iStart;
			uint8_t b = 0x80; // First unary len bit to set
			for (int hb = encodedLen; hb > 0; --hb) { // Set the sign bit and all but the last unary len bit to 1
				if (b == 0) { // Start writing a new byte if needed
					++ih;
					b = 0x80;
				}
				buf[ih] |= b;
				b >>= 1;
			}
			if (neg) // If negative, bit flip the entire encoded thing
				for (int i = iStart; i < sizeof(buf); ++i)
					buf[i] = ~buf[i];

			ar.serializeBytes(buf + iStart, encodedLen);
		}
	}
};
