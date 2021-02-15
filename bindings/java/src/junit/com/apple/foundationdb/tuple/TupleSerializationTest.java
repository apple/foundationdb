/*
 * TupleSerializationTest.java
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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.apple.foundationdb.FDBLibraryRule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests about serializing and deserializing Tuples.
 *
 * This class should be used when adding/modifying tests that involve the serialization logic,
 * while {@link TuplePackingTest}  should be used to test packing behavior. Granted,
 * that distinction is pretty arbitrary and not really clear, but the main motivation for separating the two
 * classes is just to avoid having a single ludicrously large test file, so it's probably not the end of the world
 * if this rule isn't perfectly followed.
 */
class TupleSerializationTest {
	@RegisterExtension static final FDBLibraryRule fdbLib = FDBLibraryRule.current();

	private static final byte FF = (byte)0xff;

	private static class TupleSerialization {
		private final Tuple tuple;
		private final byte[] serialization;

		TupleSerialization(Tuple tuple, byte[] serialization) {
			this.tuple = tuple;
			this.serialization = serialization;
		}
	}

	static Collection<TupleSerialization> serializedForms() {
		return Arrays.asList(
		    new TupleSerialization(Tuple.from(), new byte[0]),
		    new TupleSerialization(Tuple.from(0L), new byte[] { 0x14 }),
		    new TupleSerialization(Tuple.from(BigInteger.ZERO), new byte[] { 0x14 }),
		    new TupleSerialization(Tuple.from(1L), new byte[] { 0x15, 0x01 }),
		    new TupleSerialization(Tuple.from(BigInteger.ONE), new byte[] { 0x15, 0x01 }),
		    new TupleSerialization(Tuple.from(-1L), new byte[] { 0x13, FF - 1 }),
		    new TupleSerialization(Tuple.from(BigInteger.ONE.negate()), new byte[] { 0x13, FF - 1 }),
		    new TupleSerialization(Tuple.from(255L), new byte[] { 0x15, FF }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(255)), new byte[] { 0x15, FF }),
		    new TupleSerialization(Tuple.from(-255L), new byte[] { 0x13, 0x00 }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(-255)), new byte[] { 0x13, 0x00 }),
		    new TupleSerialization(Tuple.from(256L), new byte[] { 0x16, 0x01, 0x00 }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(256)), new byte[] { 0x16, 0x01, 0x00 }),
		    new TupleSerialization(Tuple.from(-256L), new byte[] { 0x12, FF - 1, FF }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(-256)), new byte[] { 0x12, FF - 1, FF }),
		    new TupleSerialization(Tuple.from(65536), new byte[] { 0x17, 0x01, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(-65536), new byte[] { 0x11, FF - 1, FF, FF }),
		    new TupleSerialization(Tuple.from(Long.MAX_VALUE), new byte[] { 0x1C, 0x7f, FF, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(Long.MAX_VALUE)),
		                           new byte[] { 0x1C, 0x7f, FF, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)),
		                           new byte[] { 0x1C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)),
		                           new byte[] { 0x1C, FF, FF, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(BigInteger.ONE.shiftLeft(64)),
		                           new byte[] { 0x1D, 0x09, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(-((1L << 32) - 1)), new byte[] { 0x10, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(BigInteger.ONE.shiftLeft(32).subtract(BigInteger.ONE).negate()),
		                           new byte[] { 0x10, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Long.MIN_VALUE + 2),
		                           new byte[] { 0x0C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 }),
		    new TupleSerialization(Tuple.from(Long.MIN_VALUE + 1),
		                           new byte[] { 0x0C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).add(BigInteger.ONE)),
		                           new byte[] { 0x0C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Long.MIN_VALUE), new byte[] { 0x0C, 0x7f, FF, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(Long.MIN_VALUE)),
		                           new byte[] { 0x0C, 0x7f, FF, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)),
		                           new byte[] { 0x0C, 0x7f, FF, FF, FF, FF, FF, FF, FF - 1 }),
		    new TupleSerialization(Tuple.from(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE).negate()),
		                           new byte[] { 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(3.14f), new byte[] { 0x20, (byte)0xc0, 0x48, (byte)0xf5, (byte)0xc3 }),
		    new TupleSerialization(Tuple.from(-3.14f),
		                           new byte[] { 0x20, (byte)0x3f, (byte)0xb7, (byte)0x0a, (byte)0x3c }),
		    new TupleSerialization(Tuple.from(3.14), new byte[] { 0x21, (byte)0xc0, (byte)0x09, (byte)0x1e, (byte)0xb8,
		                                                          (byte)0x51, (byte)0xeb, (byte)0x85, (byte)0x1f }),
		    new TupleSerialization(Tuple.from(-3.14), new byte[] { 0x21, (byte)0x3f, (byte)0xf6, (byte)0xe1, (byte)0x47,
		                                                           (byte)0xae, (byte)0x14, (byte)0x7a, (byte)0xe0 }),
		    new TupleSerialization(Tuple.from(0.0f), new byte[] { 0x20, (byte)0x80, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(-0.0f), new byte[] { 0x20, 0x7f, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(0.0),
		                           new byte[] { 0x21, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(-0.0), new byte[] { 0x21, 0x7f, FF, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(Float.POSITIVE_INFINITY),
		                           new byte[] { 0x20, FF, (byte)0x80, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Float.NEGATIVE_INFINITY), new byte[] { 0x20, 0x00, 0x7f, FF, FF }),
		    new TupleSerialization(Tuple.from(Double.POSITIVE_INFINITY),
		                           new byte[] { 0x21, FF, (byte)0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Double.NEGATIVE_INFINITY),
		                           new byte[] { 0x21, 0x00, 0x0f, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(Float.intBitsToFloat(Integer.MAX_VALUE)),
		                           new byte[] { 0x20, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(Double.longBitsToDouble(Long.MAX_VALUE)),
		                           new byte[] { 0x21, FF, FF, FF, FF, FF, FF, FF, FF }),
		    new TupleSerialization(Tuple.from(Float.intBitsToFloat(~0)), new byte[] { 0x20, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Double.longBitsToDouble(~0L)),
		                           new byte[] { 0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from((Object) new byte[0]), new byte[] { 0x01, 0x00 }),
		    new TupleSerialization(Tuple.from((Object) new byte[] { 0x01, 0x02, 0x03 }),
		                           new byte[] { 0x01, 0x01, 0x02, 0x03, 0x00 }),
		    new TupleSerialization(Tuple.from((Object) new byte[] { 0x00, 0x00, 0x00, 0x04 }),
		                           new byte[] { 0x01, 0x00, FF, 0x00, FF, 0x00, FF, 0x04, 0x00 }),
		    new TupleSerialization(Tuple.from(""), new byte[] { 0x02, 0x00 }),
		    new TupleSerialization(Tuple.from("hello"), new byte[] { 0x02, 'h', 'e', 'l', 'l', 'o', 0x00 }),
		    new TupleSerialization(Tuple.from("\u4e2d\u6587"),
		                           new byte[] { 0x02, (byte)0xe4, (byte)0xb8, (byte)0xad, (byte)0xe6, (byte)0x96,
		                                        (byte)0x87, 0x00 }), // chinese (three bytes per code point)
		    new TupleSerialization(Tuple.from("\u03bc\u03ac\u03b8\u03b7\u03bc\u03b1"),
		                           new byte[] { 0x02, (byte)0xce, (byte)0xbc, (byte)0xce, (byte)0xac, (byte)0xce,
		                                        (byte)0xb8, (byte)0xce, (byte)0xb7, (byte)0xce, (byte)0xbc, (byte)0xce,
		                                        (byte)0xb1, 0x00 }), // Greek (two bytes per codepoint)
		    new TupleSerialization(Tuple.from(new String(new int[] { 0x1f525 }, 0, 1)),
		                           new byte[] { 0x02, (byte)0xf0, (byte)0x9f, (byte)0x94, (byte)0xa5,
		                                        0x00 }), // fire emoji as unicode codepoint
		    new TupleSerialization(Tuple.from("\ud83d\udd25"), new byte[] { 0x02, (byte)0xf0, (byte)0x9f, (byte)0x94,
		                                                                    (byte)0xa5, 0x00 }), // fire emoji in UTF-16
		    new TupleSerialization(
		        Tuple.from("\ud83e\udd6f"),
		        new byte[] { 0x02, (byte)0xf0, (byte)0x9f, (byte)0xa5, (byte)0xaf, 0x00 }), // bagel emoji in UTF-16
		    new TupleSerialization(Tuple.from(new String(new int[] { 0x1f9a5 }, 0, 1)),
		                           new byte[] { 0x02, (byte)0xf0, (byte)0x9f, (byte)0xa6, (byte)0xa5,
		                                        0x00 }), // currently unused UTF-8 code point (will be sloth)
		    new TupleSerialization(Tuple.from("\ud83e\udda5"),
		                           new byte[] { 0x02, (byte)0xf0, (byte)0x9f, (byte)0xa6, (byte)0xa5,
		                                        0x00 }), // currently unused UTF-8 code point (will be sloth)
		    new TupleSerialization(
		        Tuple.from(new String(new int[] { 0x10FFFF }, 0, 1)),
		        new byte[] { 0x02, (byte)0xf4, (byte)0x8f, (byte)0xbf, (byte)0xbf, 0x00 }), // maximum unicode codepoint
		    new TupleSerialization(
		        Tuple.from("\udbff\udfff"),
		        new byte[] { 0x02, (byte)0xf4, (byte)0x8f, (byte)0xbf, (byte)0xbf, 0x00 }), // maximum unicode codepoint
		    new TupleSerialization(Tuple.from(Tuple.from((Object)null)), new byte[] { 0x05, 0x00, FF, 0x00 }),
		    new TupleSerialization(Tuple.from(Tuple.from(null, "hello")),
		                           new byte[] { 0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Arrays.asList(null, "hello")),
		                           new byte[] { 0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Tuple.from(null, "hell\0")),
		                           new byte[] { 0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 0x00, FF, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Arrays.asList(null, "hell\0")),
		                           new byte[] { 0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 0x00, FF, 0x00, 0x00 }),
		    new TupleSerialization(Tuple.from(Tuple.from((Object)null), "hello"),
		                           new byte[] { 0x05, 0x00, FF, 0x00, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00 }),
		    new TupleSerialization(
		        Tuple.from(Tuple.from((Object)null), "hello", new byte[] { 0x01, 0x00 }, new byte[0]),
		        new byte[] { 0x05, 0x00, FF, 0x00, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x01, 0x01, 0x00, FF, 0x00,
		                     0x01, 0x00 }),
		    new TupleSerialization(Tuple.from(new UUID(0xba5eba11, 0x5ca1ab1e)),
		                           new byte[] { 0x30, FF, FF, FF, FF, (byte)0xba, 0x5e, (byte)0xba, 0x11, 0x00, 0x00,
		                                        0x00, 0x00, 0x5c, (byte)0xa1, (byte)0xab, 0x1e }),
		    new TupleSerialization(Tuple.from(false), new byte[] { 0x26 }),
		    new TupleSerialization(Tuple.from(true), new byte[] { 0x27 }),
		    new TupleSerialization(Tuple.from((short)0x3019), new byte[] { 0x16, 0x30, 0x19 }),
		    new TupleSerialization(Tuple.from((byte)0x03), new byte[] { 0x15, 0x03 }),
		    new TupleSerialization(
		        Tuple.from(Versionstamp.complete(new byte[] { (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd,
		                                                      (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03 })),
		        new byte[] { 0x33, (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02,
		                     0x03, 0x00, 0x00 }),
		    new TupleSerialization(
		        Tuple.from(Versionstamp.complete(
		            new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a }, 657)),
		        new byte[] { 0x33, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x02, (byte)0x91 }));
	}

	@ParameterizedTest
	@MethodSource("serializedForms")
	void testSerializationPackedSize(TupleSerialization serialization) throws Exception {
		Assertions.assertEquals(serialization.serialization.length, serialization.tuple.getPackedSize(),
		                        "Incorrect packed size for tuple <" + serialization.tuple + ">");
	}

	@ParameterizedTest
	@MethodSource("serializedForms")
	void testSerializationPacking(TupleSerialization serialization) throws Exception {
		byte[] packed = serialization.tuple.pack();
		Assertions.assertArrayEquals(serialization.serialization, packed,
		                             "Incorrect packing for tuple <" + serialization.tuple + ">");
	}

	@ParameterizedTest
	@MethodSource("serializedForms")
	void testSerializationDePacking(TupleSerialization serialization) throws Exception {
		Tuple depacked = Tuple.fromItems(Tuple.fromBytes(serialization.serialization).getItems());

		Assertions.assertEquals(serialization.tuple, depacked, "Incorrect tuple after unpacking and deserialization");
	}

	/* Error handling tests on packing*/

	static final List<Tuple> offsetAndLengthTuples =
	    Arrays.asList(new Tuple(), Tuple.from((Object)null), Tuple.from(null, new byte[] { 0x10, 0x66 }),
	                  Tuple.from("dummy_string"), Tuple.from(1066L));

	@Test
	void testFromBytesWithNegativeLengthFails() throws Exception {
		Tuple allTuples = offsetAndLengthTuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// should throw
		Assertions.assertThrows(IllegalArgumentException.class, () -> { Tuple.fromBytes(allTupleBytes, 0, -1); });
	}

	@Test
	void testFromBytesWithTooLargeOffsetFails() throws Exception {
		Tuple allTuples = offsetAndLengthTuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// should throw
		Assertions.assertThrows(IllegalArgumentException.class,
		                        () -> { Tuple.fromBytes(allTupleBytes, allTupleBytes.length + 1, 4); });
	}

	@Test
	void testFromBytesWithTooLargeLengthFails() throws Exception {
		Tuple allTuples = offsetAndLengthTuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// should throw
		Assertions.assertThrows(IllegalArgumentException.class,
		                        () -> { Tuple.fromBytes(allTupleBytes, 0, allTupleBytes.length + 1); });
	}

	@Test
	void testNotAbleToExceedArrayLengthInFromBytes() throws Exception {
		Tuple allTuples = offsetAndLengthTuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// should throw
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			Tuple.fromBytes(allTupleBytes, allTupleBytes.length / 2, allTupleBytes.length / 2 + 2);
		});
	}

	@Test
	void testEmptyAtEndTuple() throws Exception {
		Tuple allTuples = offsetAndLengthTuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// Allow an offset to equal the length of the array, but essentially only a
		// zero-length is allowed there.
		Tuple emptyAtEndTuple = Tuple.fromBytes(allTupleBytes, allTupleBytes.length, 0);
		Assertions.assertTrue(emptyAtEndTuple.isEmpty(), "tuple with no bytes is not empty");
	}

	@Test
	void testFromBytesWithNegativeOffsetFails() throws Exception {
		Tuple allTuples = offsetAndLengthTuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// should throw
		Assertions.assertThrows(IllegalArgumentException.class, () -> { Tuple.fromBytes(allTupleBytes, -1, 4); });
	}

	@Test
	void testUnpackedTupleMatchesSerializedTuple() throws Exception {
		Tuple allTuples = offsetAndLengthTuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// Unpack each tuple individually using their lengths
		int offset = 0;
		for (Tuple t : offsetAndLengthTuples) {
			int length = t.getPackedSize();
			Tuple unpacked = Tuple.fromBytes(allTupleBytes, offset, length);
			Assertions.assertEquals(t, unpacked,
			                        "unpacked tuple " + unpacked + " does not match serialized tuple " + t);
			offset += length;
		}
	}

	@Test
	void testUnpackedTupleMatchesCombinedTuple() throws Exception {
		List<Tuple> tuples = offsetAndLengthTuples;
		Tuple allTuples = tuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// Unpack successive pairs of tuples.
		int offset = 0;
		for (int i = 0; i < tuples.size() - 1; i++) {
			Tuple combinedTuple = tuples.get(i).addAll(tuples.get(i + 1));
			Tuple unpacked = Tuple.fromBytes(allTupleBytes, offset, combinedTuple.getPackedSize());
			Assertions.assertEquals(unpacked, combinedTuple,
			                        "unpacked tuple " + unpacked + " does not match combined tuple " + combinedTuple);
			offset += tuples.get(i).getPackedSize();
		}
	}

	@Test
	void testPackIntoBuffer() throws Exception {
		Tuple t = Tuple.from("hello", 3.14f, "world");

		ByteBuffer buffer = ByteBuffer.allocate("hello".length() + 2 + Float.BYTES + 1 + "world".length() + 2);
		t.packInto(buffer);
		Assertions.assertArrayEquals(buffer.array(), t.pack(), "buffer and tuple do not match");

		buffer = ByteBuffer.allocate(t.getPackedSize() + 2);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		t.packInto(buffer);
		Assertions.assertArrayEquals(ByteArrayUtil.join(t.pack(), new byte[] { 0x00, 0x00 }), buffer.array(),
		                             "buffer and tuple do not match");
		Assertions.assertEquals(ByteOrder.LITTLE_ENDIAN, buffer.order(), "byte order changed");

		buffer = ByteBuffer.allocate(t.getPackedSize() + 2);
		buffer.put((byte)0x01).put((byte)0x02);
		t.packInto(buffer);
		Assertions.assertArrayEquals(t.pack(new byte[] { 0x01, 0x02 }), buffer.array(),
		                             "buffer and tuple do not match");

		buffer = ByteBuffer.allocate(t.getPackedSize() - 1);
		try {
			t.packInto(buffer);
			Assertions.fail("able to pack into buffer that was too small");
		} catch (BufferOverflowException expected) {
		}

		Tuple tCopy = Tuple.fromItems(t.getItems()); // remove memoized stuff
		buffer = ByteBuffer.allocate(t.getPackedSize() - 1);
		try {
			tCopy.packInto(buffer);
			Assertions.fail("able to pack into buffer that was too small");
		} catch (BufferOverflowException expected) {
		}

		Tuple tWithIncomplete = Tuple.from(Versionstamp.incomplete(3));
		buffer = ByteBuffer.allocate(tWithIncomplete.getPackedSize());
		try {
			tWithIncomplete.packInto(buffer);
			Assertions.fail("able to pack incomplete versionstamp into buffer");
		} catch (IllegalArgumentException expected) {
		}

		Assertions.assertEquals(0, buffer.arrayOffset(),
		                        "offset changed after unsuccessful pack with incomplete versionstamp");
	}
}
