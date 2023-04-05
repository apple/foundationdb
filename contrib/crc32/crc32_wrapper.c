/*
 * Copyright (C) 2015 Anton Blanchard <anton@au.ibm.com>, IBM
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of either:
 *
 *  a) the GNU General Public License as published by the Free Software
 *     Foundation; either version 2 of the License, or (at your option)
 *     any later version, or
 *  b) the Apache License, Version 2.0
 */

#define CRC_TABLE

#ifdef CRC32_CONSTANTS_HEADER
#include CRC32_CONSTANTS_HEADER
#else
#include "crc32/crc32_constants.h"
#endif

#define VMX_ALIGN 16
#define VMX_ALIGN_MASK (VMX_ALIGN - 1)

#ifdef REFLECT
static unsigned int crc32_align(unsigned int crc, unsigned char* p, unsigned long len) {
	while (len--)
		crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
	return crc;
}
#else
static unsigned int crc32_align(unsigned int crc, unsigned char* p, unsigned long len) {
	while (len--)
		crc = crc_table[((crc >> 24) ^ *p++) & 0xff] ^ (crc << 8);
	return crc;
}
#endif

#ifndef CRC32_FUNCTION
#define CRC32_FUNCTION crc32_vpmsum
#endif
#ifndef CRC32_FUNCTION_ASM
#define CRC32_FUNCTION_ASM __crc32_vpmsum
#endif

unsigned int CRC32_FUNCTION_ASM(unsigned int crc, unsigned char* p, unsigned long len);

unsigned int CRC32_FUNCTION(unsigned int crc, unsigned char* p, unsigned long len) {
#ifdef __powerpc64 // avoid link failures on systems without CRC32_FUNCTION_ASM declared
	unsigned int prealign;
	unsigned int tail;

#ifdef CRC_XOR
	crc ^= 0xffffffff;
#endif

	if (len < VMX_ALIGN + VMX_ALIGN_MASK) {
		crc = crc32_align(crc, p, len);
		goto out;
	}

	if ((unsigned long)p & VMX_ALIGN_MASK) {
		prealign = VMX_ALIGN - ((unsigned long)p & VMX_ALIGN_MASK);
		crc = crc32_align(crc, p, prealign);
		len -= prealign;
		p += prealign;
	}

	crc = CRC32_FUNCTION_ASM(crc, p, len & ~VMX_ALIGN_MASK);

	tail = len & VMX_ALIGN_MASK;
	if (tail) {
		p += len & ~VMX_ALIGN_MASK;
		crc = crc32_align(crc, p, tail);
	}

out:
#ifdef CRC_XOR
	crc ^= 0xffffffff;
#endif
#endif
	return crc;
}
