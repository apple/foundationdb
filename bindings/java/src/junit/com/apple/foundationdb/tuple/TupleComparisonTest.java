/*
 * TupleComparisonTest.java
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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for comparisons of tuple objects.
 */
class TupleComparisonTest {

	static List<Tuple> comparisons = Arrays.asList(
	    Tuple.from(0L), Tuple.from(BigInteger.ZERO), Tuple.from(1L), Tuple.from(BigInteger.ONE), Tuple.from(-1L),
	    Tuple.from(BigInteger.ONE.negate()), Tuple.from(Long.MAX_VALUE), Tuple.from(Long.MIN_VALUE),
	    Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)),
	    Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).shiftLeft(1)), Tuple.from(-0.0f), Tuple.from(0.0f),
	    Tuple.from(-0.0), Tuple.from(0.0), Tuple.from(Float.NEGATIVE_INFINITY), Tuple.from(Double.NEGATIVE_INFINITY),
	    Tuple.from(Float.NaN), Tuple.from(Double.NaN),
	    Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) + 1)),
	    Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) + 1)),
	    Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) + 2)),
	    Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) + 2)),
	    Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) ^ Integer.MIN_VALUE)),
	    Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) ^ Long.MIN_VALUE)),
	    Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) ^ Integer.MIN_VALUE + 1)),
	    Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) ^ Long.MIN_VALUE + 1)),
	    Tuple.from(Float.POSITIVE_INFINITY), Tuple.from(Double.POSITIVE_INFINITY), Tuple.from((Object) new byte[0]),
	    Tuple.from((Object) new byte[] { 0x00 }), Tuple.from((Object) new byte[] { 0x00, (byte)0xFF }),
	    Tuple.from((Object) new byte[] { 0x7f }), Tuple.from((Object) new byte[] { (byte)0x80 }),
	    Tuple.from(null, new byte[0]), Tuple.from(null, new byte[] { 0x00 }),
	    Tuple.from(null, new byte[] { 0x00, (byte)0xFF }), Tuple.from(null, new byte[] { 0x7f }),
	    Tuple.from(null, new byte[] { (byte)0x80 }), Tuple.from(Tuple.from(null, new byte[0])),
	    Tuple.from(Tuple.from(null, new byte[] { 0x00 })),
	    Tuple.from(Tuple.from(null, new byte[] { 0x00, (byte)0xFF })),
	    Tuple.from(Tuple.from(null, new byte[] { 0x7f })), Tuple.from(Tuple.from(null, new byte[] { (byte)0x80 })),
	    Tuple.from("a"), Tuple.from("\u03bc\u03ac\u03b8\u03b7\u03bc\u03b1"),
	    Tuple.from("\u03bc\u03b1\u0301\u03b8\u03b7\u03bc\u03b1"), Tuple.from("\u4e2d\u6587"),
	    Tuple.from("\u4e2d\u570B"), Tuple.from("\ud83d\udd25"), Tuple.from("\ud83e\udd6f"), Tuple.from("a\ud83d\udd25"),
	    Tuple.from("\ufb49"), Tuple.from("\ud83d\udd25\ufb49"), Tuple.from(new UUID(-1, 0)),
	    Tuple.from(new UUID(-1, -1)), Tuple.from(new UUID(1, -1)), Tuple.from(new UUID(1, 1)), Tuple.from(false),
	    Tuple.from(true), Tuple.from(Arrays.asList(0, 1, 2)), Tuple.from(Arrays.asList(0, 1), "hello"),
	    Tuple.from(Arrays.asList(0, 1), "help"),
	    Tuple.from(Versionstamp.complete(
	        new byte[] { 0x0a, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, (byte)0xFF, 0x00, 0x01, 0x02, 0x03 })),
	    Tuple.from(Versionstamp.complete(new byte[] { (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee,
	                                                  (byte)0xFF, 0x00, 0x01, 0x02, 0x03 })),
	    Tuple.from(Versionstamp.complete(new byte[] { (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee,
	                                                  (byte)0xFF, 0x00, 0x01, 0x02, 0x03 },
	                                     1)),
	    Tuple.from(Versionstamp.complete(new byte[] { (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee,
	                                                  (byte)0xFF, 0x00, 0x01, 0x02, 0x03 },
	                                     0xa101)),
	    Tuple.from(Versionstamp.complete(new byte[] { (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee,
	                                                  (byte)0xFF, 0x00, 0x01, 0x02, 0x03 },
	                                     65535)));

	static Stream<Arguments> cartesianProvider() {
		List<Arguments> args = new ArrayList<>(comparisons.size() * comparisons.size());
		for (Tuple l : comparisons) {
			for (Tuple r : comparisons) {
				args.add(Arguments.of(l, r));
			}
		}
		return args.stream();
	}

	@ParameterizedTest
	@MethodSource("cartesianProvider")
	void testCanCompare(Tuple l, Tuple r) {
		/*
		 * Verify that both implementations of the comparator compare the same way
		 */
		Tuple t1copy = Tuple.fromList(l.getItems());
		Tuple t2copy = Tuple.fromList(r.getItems());
		int semanticComparison = t1copy.compareTo(t2copy);
		int byteComparison = ByteArrayUtil.compareUnsigned(l.pack(), r.pack());
		String errorMsg = String.format("tuple l and r comparisons mismatched; semantic: <%d>,byte: <%d>",
		                                semanticComparison, byteComparison);
		Assertions.assertEquals(Integer.signum(semanticComparison), Integer.signum(byteComparison), errorMsg);
		int implicitByteComparison = l.compareTo(r);
		Assertions.assertEquals(Integer.signum(semanticComparison), Integer.signum(implicitByteComparison), errorMsg);
	}
}
