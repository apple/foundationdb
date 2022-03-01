/*
  Copyright (c) 2013 - 2014 Mark Adler, Robert Vazan

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the author be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
  claim that you wrote the original software. If you use this software
  in a product, an acknowledgment in the product documentation would be
  appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
  misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.


  THIS CODE HAS BEEN ALTERED FROM THE ORIGINAL
*/

#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "flow/crc32c.h"

#if !defined(__aarch64__) && !defined(__powerpc64__)
#include <nmmintrin.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <random>
#include <algorithm>
#include "flow/Platform.h"
#include "crc32c-generated-constants.cpp"

[[maybe_unused]] static uint32_t append_trivial(uint32_t crc, const uint8_t* input, size_t length) {
	for (size_t i = 0; i < length; ++i) {
		crc = crc ^ input[i];
		for (int j = 0; j < 8; j++)
			crc = (crc >> 1) ^ 0x80000000 ^ ((~crc & 1) * POLY);
	}
	return crc;
}

/* Table-driven software version as a fall-back.  This is about 15 times slower
   than using the hardware instructions.  This assumes little-endian integers,
   as is the case on Intel processors that the assembler code here is for. */
[[maybe_unused]] static uint32_t append_adler_table(uint32_t crci, const uint8_t* input, size_t length) {
	const uint8_t* next = input;
	uint64_t crc;

	crc = crci ^ 0xffffffff;
	while (length && ((uintptr_t)next & 7) != 0) {
		crc = table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
		--length;
	}
	while (length >= 8) {
		crc ^= *(uint64_t*)next;
		crc = table[7][crc & 0xff] ^ table[6][(crc >> 8) & 0xff] ^ table[5][(crc >> 16) & 0xff] ^
		      table[4][(crc >> 24) & 0xff] ^ table[3][(crc >> 32) & 0xff] ^ table[2][(crc >> 40) & 0xff] ^
		      table[1][(crc >> 48) & 0xff] ^ table[0][crc >> 56];
		next += 8;
		length -= 8;
	}
	while (length) {
		crc = table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
		--length;
	}
	return (uint32_t)crc ^ 0xffffffff;
}

/* Table-driven software version as a fall-back.  This is about 15 times slower
   than using the hardware instructions.  This assumes little-endian integers,
   as is the case on Intel processors that the assembler code here is for. */
static uint32_t append_table(uint32_t crci, const uint8_t* input, size_t length) {
	const uint8_t* next = input;
#ifdef _M_X64
	uint64_t crc;
#else
	uint32_t crc;
#endif

	crc = crci ^ 0xffffffff;
#ifdef _M_X64
	while (length && ((uintptr_t)next & 7) != 0) {
		crc = table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
		--length;
	}
	while (length >= 16) {
		crc ^= *(uint64_t*)next;
		uint64_t high = *(uint64_t*)(next + 8);
		crc = table[15][crc & 0xff] ^ table[14][(crc >> 8) & 0xff] ^ table[13][(crc >> 16) & 0xff] ^
		      table[12][(crc >> 24) & 0xff] ^ table[11][(crc >> 32) & 0xff] ^ table[10][(crc >> 40) & 0xff] ^
		      table[9][(crc >> 48) & 0xff] ^ table[8][crc >> 56] ^ table[7][high & 0xff] ^
		      table[6][(high >> 8) & 0xff] ^ table[5][(high >> 16) & 0xff] ^ table[4][(high >> 24) & 0xff] ^
		      table[3][(high >> 32) & 0xff] ^ table[2][(high >> 40) & 0xff] ^ table[1][(high >> 48) & 0xff] ^
		      table[0][high >> 56];
		next += 16;
		length -= 16;
	}
#else
	while (length && ((uintptr_t)next & 3) != 0) {
		crc = table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
		--length;
	}
	while (length >= 12) {
		crc ^= *(uint32_t*)next;
		uint32_t high = *(uint32_t*)(next + 4);
		uint32_t high2 = *(uint32_t*)(next + 8);
		crc = table[11][crc & 0xff] ^ table[10][(crc >> 8) & 0xff] ^ table[9][(crc >> 16) & 0xff] ^
		      table[8][crc >> 24] ^ table[7][high & 0xff] ^ table[6][(high >> 8) & 0xff] ^
		      table[5][(high >> 16) & 0xff] ^ table[4][high >> 24] ^ table[3][high2 & 0xff] ^
		      table[2][(high2 >> 8) & 0xff] ^ table[1][(high2 >> 16) & 0xff] ^ table[0][high2 >> 24];
		next += 12;
		length -= 12;
	}
#endif
	while (length) {
		crc = table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
		--length;
	}
	return (uint32_t)crc ^ 0xffffffff;
}

/* Apply the zeros operator table to crc. */
static inline uint32_t shift_crc(uint32_t shift_table[][256], uint32_t crc) {
	return shift_table[0][crc & 0xff] ^ shift_table[1][(crc >> 8) & 0xff] ^ shift_table[2][(crc >> 16) & 0xff] ^
	       shift_table[3][crc >> 24];
}

/* Compute CRC-32C using the hardware instruction. */
#if (defined(__clang__) || defined(__GNUG__) && !defined(__aarch64__) && !defined(__powerpc64__))
/* Enable SSE CEC instructions on Intel processor */
__attribute__((target("sse4.2")))
#endif
#ifndef __powerpc64__
static uint32_t
append_hw(uint32_t crc, const uint8_t* buf, size_t len) {
	const uint8_t* next = buf;
	const uint8_t* end;
#ifdef _M_X64
	uint64_t crc0, crc1, crc2; /* need to be 64 bits for crc32q */
#else
	uint32_t crc0, crc1, crc2;
#endif

	/* pre-process the crc */
	crc0 = crc ^ 0xffffffff;

	/* compute the crc for up to seven leading bytes to bring the data pointer
	   to an eight-byte boundary */
	while (len && ((uintptr_t)next & 7) != 0) {
		crc0 = hwCrc32cU8(static_cast<uint32_t>(crc0), *next);
		++next;
		--len;
	}

#ifdef _M_X64
	/* compute the crc on sets of LONG_SHIFT*3 bytes, executing three independent crc
	   instructions, each on LONG_SHIFT bytes -- this is optimized for the Nehalem,
	   Westmere, Sandy Bridge, and Ivy Bridge architectures, which have a
	   throughput of one crc per cycle, but a latency of three cycles */
	while (len >= 3 * LONG_SHIFT) {
		crc1 = 0;
		crc2 = 0;
		end = next + LONG_SHIFT;
		do {
			crc0 = hwCrc32cU64(crc0, *reinterpret_cast<const uint64_t*>(next));
			crc1 = hwCrc32cU64(crc1, *reinterpret_cast<const uint64_t*>(next + LONG_SHIFT));
			crc2 = hwCrc32cU64(crc2, *reinterpret_cast<const uint64_t*>(next + 2 * LONG_SHIFT));
			next += 8;
		} while (next < end);
		crc0 = shift_crc(long_shifts, static_cast<uint32_t>(crc0)) ^ crc1;
		crc0 = shift_crc(long_shifts, static_cast<uint32_t>(crc0)) ^ crc2;
		next += 2 * LONG_SHIFT;
		len -= 3 * LONG_SHIFT;
	}

	/* do the same thing, but now on SHORT_SHIFT*3 blocks for the remaining data less
	   than a LONG_SHIFT*3 block */
	while (len >= 3 * SHORT_SHIFT) {
		crc1 = 0;
		crc2 = 0;
		end = next + SHORT_SHIFT;
		do {
			crc0 = hwCrc32cU64(crc0, *reinterpret_cast<const uint64_t*>(next));
			crc1 = hwCrc32cU64(crc1, *reinterpret_cast<const uint64_t*>(next + SHORT_SHIFT));
			crc2 = hwCrc32cU64(crc2, *reinterpret_cast<const uint64_t*>(next + 2 * SHORT_SHIFT));
			next += 8;
		} while (next < end);
		crc0 = shift_crc(short_shifts, static_cast<uint32_t>(crc0)) ^ crc1;
		crc0 = shift_crc(short_shifts, static_cast<uint32_t>(crc0)) ^ crc2;
		next += 2 * SHORT_SHIFT;
		len -= 3 * SHORT_SHIFT;
	}

	/* compute the crc on the remaining eight-byte units less than a SHORT_SHIFT*3
	block */
	end = next + (len - (len & 7));
	while (next < end) {
		crc0 = hwCrc32cU64(crc0, *reinterpret_cast<const uint64_t*>(next));
		next += 8;
	}
#else
	/* compute the crc on sets of LONG_SHIFT*3 bytes, executing three independent crc
	instructions, each on LONG_SHIFT bytes -- this is optimized for the Nehalem,
	Westmere, Sandy Bridge, and Ivy Bridge architectures, which have a
	throughput of one crc per cycle, but a latency of three cycles */
	while (len >= 3 * LONG_SHIFT) {
		crc1 = 0;
		crc2 = 0;
		end = next + LONG_SHIFT;
		do {
			crc0 = hwCrc32cU32(crc0, *reinterpret_cast<const uint32_t*>(next));
			crc1 = hwCrc32cU32(crc1, *reinterpret_cast<const uint32_t*>(next + LONG_SHIFT));
			crc2 = hwCrc32cU32(crc2, *reinterpret_cast<const uint32_t*>(next + 2 * LONG_SHIFT));
			next += 4;
		} while (next < end);
		crc0 = shift_crc(long_shifts, static_cast<uint32_t>(crc0)) ^ crc1;
		crc0 = shift_crc(long_shifts, static_cast<uint32_t>(crc0)) ^ crc2;
		next += 2 * LONG_SHIFT;
		len -= 3 * LONG_SHIFT;
	}

	/* do the same thing, but now on SHORT_SHIFT*3 blocks for the remaining data less
	than a LONG_SHIFT*3 block */
	while (len >= 3 * SHORT_SHIFT) {
		crc1 = 0;
		crc2 = 0;
		end = next + SHORT_SHIFT;
		do {
			crc0 = hwCrc32cU32(crc0, *reinterpret_cast<const uint32_t*>(next));
			crc1 = hwCrc32cU32(crc1, *reinterpret_cast<const uint32_t*>(next + SHORT_SHIFT));
			crc2 = hwCrc32cU32(crc2, *reinterpret_cast<const uint32_t*>(next + 2 * SHORT_SHIFT));
			next += 4;
		} while (next < end);
		crc0 = shift_crc(short_shifts, static_cast<uint32_t>(crc0)) ^ crc1;
		crc0 = shift_crc(short_shifts, static_cast<uint32_t>(crc0)) ^ crc2;
		next += 2 * SHORT_SHIFT;
		len -= 3 * SHORT_SHIFT;
	}

	/* compute the crc on the remaining eight-byte units less than a SHORT_SHIFT*3
	block */
	end = next + (len - (len & 7));
	while (next < end) {
		crc0 = hwCrc32cU32(crc0, *reinterpret_cast<const uint32_t*>(next));
		next += 4;
	}
#endif
	len &= 7;

	/* compute the crc for up to seven trailing bytes */
	while (len) {
		crc0 = hwCrc32cU8(static_cast<uint32_t>(crc0), *next);
		++next;
		--len;
	}

	/* return a post-processed crc */
	return static_cast<uint32_t>(crc0) ^ 0xffffffff;
}
#endif

static bool hw_available = platform::isHwCrcSupported();

extern "C" uint32_t crc32c_append(uint32_t crc, const uint8_t* input, size_t length) {
#ifndef __powerpc64__
	if (hw_available)
		return append_hw(crc, input, length);
	else
#endif
		return append_table(crc, input, length);
}
