/*
 * CompressedInt.actor.cpp
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

#include "flow/UnitTest.h"
#include "flow/CompressedInt.h"

void printBitsLittle(size_t const size, void const* const ptr) {
	unsigned char* b = (unsigned char*)ptr;
	unsigned char byte;
	int i, j;

	for (i = size - 1; i >= 0; i--) {
		for (j = 7; j >= 0; j--) {
			byte = (b[i] >> j) & 1;
			printf("%u", byte);
		}
		printf(" ");
	}
	puts("");
}

void printBitsBig(size_t const size, void const* const ptr) {
	unsigned char* b = (unsigned char*)ptr;
	unsigned char byte;
	int i, j;

	for (i = 0; i < size; ++i) {
		for (j = 7; j >= 0; j--) {
			byte = (b[i] >> j) & 1;
			printf("%u", byte);
		}
		printf(" ");
	}
	puts("");
}

template <typename IntType>
void testCompressedInt(IntType n, StringRef rep = StringRef()) {
	BinaryWriter w(AssumeVersion(g_network->protocolVersion()));
	CompressedInt<IntType> cn(n);

	w << cn;
	if (rep.size() != 0 && w.toValue() != rep) {
		printf("WRONG ENCODING:\n");
		printf("  test value (BigE):  ");
		printBitsLittle(sizeof(IntType), &n);
		printf("  encoded:            ");
		printBitsBig(w.toValue().size(), w.toValue().begin());
		printf("    expected:         ");
		printBitsBig(rep.size(), rep.begin());
		puts("");
	} else
		rep = w.toValue();

	cn.value = 0;
	BinaryReader r(rep, AssumeVersion(g_network->protocolVersion()));
	r >> cn;

	if (cn.value != n) {
		printf("FAILURE:\n");
		printf("  test value: (Big): ");
		printBitsLittle(sizeof(IntType), &n);
		printf("  encoded:           ");
		printBitsBig(rep.size(), rep.begin());
		printf("  decoded value:     ");
		printBitsLittle(sizeof(IntType), &cn.value);
		puts("");
	}
}

TEST_CASE("/flow/compressed_ints") {
	testCompressedInt<int>(-2, LiteralStringRef("\x7e"));
	testCompressedInt<int>(-1, LiteralStringRef("\x7f"));
	testCompressedInt<int>(0, LiteralStringRef("\x80"));
	testCompressedInt<int>(1, LiteralStringRef("\x81"));
	testCompressedInt<int>(2, LiteralStringRef("\x82"));
	testCompressedInt<int64_t>(0x4000000000000000, LiteralStringRef("\xFF\xC0\x40\x00\x00\x00\x00\x00\x00\x00"));

	int64_t n = 0;
	for (int i = 0; i < 10000000; ++i) {
		n <<= 1;
		if (deterministicRandom()->coinflip())
			n |= 1;
		testCompressedInt<int64_t>(n);
		testCompressedInt<int32_t>(n);
		testCompressedInt<int16_t>(n);
	}
	return Void();
}
