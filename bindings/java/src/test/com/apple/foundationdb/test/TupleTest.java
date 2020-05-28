/*
 * TupleTest.java
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

package com.apple.foundationdb.test;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

public class TupleTest {
	private static final byte FF = (byte)0xff;

	public static void main(String[] args) throws NoSuchFieldException {
		final int reps = 1000;
		try {
			FDB fdb = FDB.selectAPIVersion(700);
			addMethods();
			comparisons();
			emptyTuple();
			incompleteVersionstamps();
			intoBuffer();
			offsetsAndLengths();
			malformedBytes();
			malformedStrings();
			replaceTests();
			serializedForms();
			try(Database db = fdb.open()) {
				runTests(reps, db);
			}
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}

	private static class TupleSerialization {
		private final Tuple tuple;
		private final byte[] serialization;

		TupleSerialization(Tuple tuple, byte[] serialization) {
			this.tuple = tuple;
			this.serialization = serialization;
		}

		static void addAll(List<TupleSerialization> list, Object... args) {
			for(int i = 0; i < args.length; i += 2) {
				TupleSerialization serialization = new TupleSerialization((Tuple)args[i], (byte[])args[i + 1]);
				list.add(serialization);
			}
		}
	}

	private static void serializedForms() {
		List<TupleSerialization> serializations = new ArrayList<>();
		TupleSerialization.addAll(serializations,
				Tuple.from(), new byte[0],
				Tuple.from(0L), new byte[]{0x14},
				Tuple.from(BigInteger.ZERO), new byte[]{0x14},
				Tuple.from(1L), new byte[]{0x15, 0x01},
				Tuple.from(BigInteger.ONE), new byte[]{0x15, 0x01},
				Tuple.from(-1L), new byte[]{0x13, FF - 1},
				Tuple.from(BigInteger.ONE.negate()), new byte[]{0x13, FF - 1},
				Tuple.from(255L), new byte[]{0x15, FF},
				Tuple.from(BigInteger.valueOf(255)), new byte[]{0x15, FF},
				Tuple.from(-255L), new byte[]{0x13, 0x00},
				Tuple.from(BigInteger.valueOf(-255)), new byte[]{0x13, 0x00},
				Tuple.from(256L), new byte[]{0x16, 0x01, 0x00},
				Tuple.from(BigInteger.valueOf(256)), new byte[]{0x16, 0x01, 0x00},
				Tuple.from(-256L), new byte[]{0x12, FF - 1, FF},
				Tuple.from(BigInteger.valueOf(-256)), new byte[]{0x12, FF - 1, FF},
				Tuple.from(65536), new byte[]{0x17, 0x01, 0x00, 0x00},
				Tuple.from(-65536), new byte[]{0x11, FF - 1, FF, FF},
				Tuple.from(Long.MAX_VALUE), new byte[]{0x1C, 0x7f, FF, FF, FF, FF, FF, FF, FF},
				Tuple.from(BigInteger.valueOf(Long.MAX_VALUE)), new byte[]{0x1C, 0x7f, FF, FF, FF, FF, FF, FF, FF},
				Tuple.from(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)), new byte[]{0x1C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE)), new byte[]{0x1C, FF, FF, FF, FF, FF, FF, FF, FF},
				Tuple.from(BigInteger.ONE.shiftLeft(64)), new byte[]{0x1D, 0x09, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(-((1L << 32) - 1)), new byte[]{0x10, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(BigInteger.ONE.shiftLeft(32).subtract(BigInteger.ONE).negate()), new byte[]{0x10, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(Long.MIN_VALUE + 2), new byte[]{0x0C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
				Tuple.from(Long.MIN_VALUE + 1), new byte[]{0x0C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).add(BigInteger.ONE)), new byte[]{0x0C, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(Long.MIN_VALUE), new byte[]{0x0C, 0x7f, FF, FF, FF, FF, FF, FF, FF},
				Tuple.from(BigInteger.valueOf(Long.MIN_VALUE)), new byte[]{0x0C, 0x7f, FF, FF, FF, FF, FF, FF, FF},
				Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)), new byte[]{0x0C, 0x7f, FF, FF, FF, FF, FF, FF, FF - 1},
				Tuple.from(BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE).negate()), new byte[]{0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(3.14f), new byte[]{0x20, (byte)0xc0, 0x48, (byte)0xf5, (byte)0xc3},
				Tuple.from(-3.14f), new byte[]{0x20, (byte)0x3f, (byte)0xb7, (byte)0x0a, (byte)0x3c},
				Tuple.from(3.14), new byte[]{0x21, (byte)0xc0, (byte)0x09, (byte)0x1e, (byte)0xb8, (byte)0x51, (byte)0xeb, (byte)0x85, (byte)0x1f},
				Tuple.from(-3.14), new byte[]{0x21, (byte)0x3f, (byte)0xf6, (byte)0xe1, (byte)0x47, (byte)0xae, (byte)0x14, (byte)0x7a, (byte)0xe0},
				Tuple.from(0.0f), new byte[]{0x20, (byte)0x80, 0x00, 0x00, 0x00},
				Tuple.from(-0.0f), new byte[]{0x20, 0x7f, FF, FF, FF},
				Tuple.from(0.0), new byte[]{0x21, (byte)0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(-0.0), new byte[]{0x21, 0x7f, FF, FF, FF, FF, FF, FF, FF},
				Tuple.from(Float.POSITIVE_INFINITY), new byte[]{0x20, FF, (byte)0x80, 0x00, 0x00},
				Tuple.from(Float.NEGATIVE_INFINITY), new byte[]{0x20, 0x00, 0x7f, FF, FF},
				Tuple.from(Double.POSITIVE_INFINITY), new byte[]{0x21, FF, (byte)0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(Double.NEGATIVE_INFINITY), new byte[]{0x21, 0x00, 0x0f, FF, FF, FF, FF, FF, FF},
				Tuple.from(Float.intBitsToFloat(Integer.MAX_VALUE)), new byte[]{0x20, FF, FF, FF, FF},
				Tuple.from(Double.longBitsToDouble(Long.MAX_VALUE)), new byte[]{0x21, FF, FF, FF, FF, FF, FF, FF, FF},
				Tuple.from(Float.intBitsToFloat(~0)), new byte[]{0x20, 0x00, 0x00, 0x00, 0x00},
				Tuple.from(Double.longBitsToDouble(~0L)), new byte[]{0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				Tuple.from((Object)new byte[0]), new byte[]{0x01, 0x00},
				Tuple.from((Object)new byte[]{0x01, 0x02, 0x03}), new byte[]{0x01, 0x01, 0x02, 0x03, 0x00},
				Tuple.from((Object)new byte[]{0x00, 0x00, 0x00, 0x04}), new byte[]{0x01, 0x00, FF, 0x00, FF, 0x00, FF, 0x04, 0x00},
				Tuple.from(""), new byte[]{0x02, 0x00},
				Tuple.from("hello"), new byte[]{0x02, 'h', 'e', 'l', 'l', 'o', 0x00},
				Tuple.from("\u4e2d\u6587"), new byte[]{0x02, (byte)0xe4, (byte)0xb8, (byte)0xad, (byte)0xe6, (byte)0x96, (byte)0x87, 0x00}, // chinese (three bytes per code point)
				Tuple.from("\u03bc\u03ac\u03b8\u03b7\u03bc\u03b1"), new byte[]{0x02, (byte)0xce, (byte)0xbc, (byte)0xce, (byte)0xac, (byte)0xce, (byte)0xb8, (byte)0xce, (byte)0xb7, (byte)0xce, (byte)0xbc, (byte)0xce, (byte)0xb1, 0x00}, // Greek (two bytes per codepoint)
				Tuple.from(new String(new int[]{0x1f525}, 0, 1)), new byte[]{0x02, (byte)0xf0, (byte)0x9f, (byte)0x94, (byte)0xa5, 0x00}, // fire emoji as unicode codepoint
				Tuple.from("\ud83d\udd25"), new byte[]{0x02, (byte)0xf0, (byte)0x9f, (byte)0x94, (byte)0xa5, 0x00}, // fire emoji in UTF-16
				Tuple.from("\ud83e\udd6f"), new byte[]{0x02, (byte)0xf0, (byte)0x9f, (byte)0xa5, (byte)0xaf, 0x00}, // bagel emoji in UTF-16
				Tuple.from(new String(new int[]{0x1f9a5}, 0, 1)), new byte[]{0x02, (byte)0xf0, (byte)0x9f, (byte)0xa6, (byte)0xa5, 0x00}, // currently unused UTF-8 code point (will be sloth)
				Tuple.from("\ud83e\udda5"), new byte[]{0x02, (byte)0xf0, (byte)0x9f, (byte)0xa6, (byte)0xa5, 0x00}, // currently unused UTF-8 code point (will be sloth)
				Tuple.from(new String(new int[]{0x10FFFF}, 0, 1)), new byte[]{0x02, (byte)0xf4, (byte)0x8f, (byte)0xbf, (byte)0xbf, 0x00}, // maximum unicode codepoint
				Tuple.from("\udbff\udfff"), new byte[]{0x02, (byte)0xf4, (byte)0x8f, (byte)0xbf, (byte)0xbf, 0x00}, // maximum unicode codepoint
				Tuple.from(Tuple.from((Object)null)), new byte[]{0x05, 0x00, FF, 0x00},
				Tuple.from(Tuple.from(null, "hello")), new byte[]{0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00},
				Tuple.from(Arrays.asList(null, "hello")), new byte[]{0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00},
				Tuple.from(Tuple.from(null, "hell\0")), new byte[]{0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 0x00, FF, 0x00, 0x00},
				Tuple.from(Arrays.asList(null, "hell\0")), new byte[]{0x05, 0x00, FF, 0x02, 'h', 'e', 'l', 'l', 0x00, FF, 0x00, 0x00},
				Tuple.from(Tuple.from((Object)null), "hello"), new byte[]{0x05, 0x00, FF, 0x00, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00},
				Tuple.from(Tuple.from((Object)null), "hello", new byte[]{0x01, 0x00}, new byte[0]), new byte[]{0x05, 0x00, FF, 0x00, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x01, 0x01, 0x00, FF, 0x00, 0x01, 0x00},
				Tuple.from(new UUID(0xba5eba11, 0x5ca1ab1e)), new byte[]{0x30, FF, FF, FF, FF, (byte)0xba, 0x5e, (byte)0xba, 0x11, 0x00, 0x00, 0x00, 0x00, 0x5c, (byte)0xa1, (byte)0xab, 0x1e},
				Tuple.from(false), new byte[]{0x26},
				Tuple.from(true), new byte[]{0x27},
				Tuple.from((short)0x3019), new byte[]{0x16, 0x30, 0x19},
				Tuple.from((byte)0x03), new byte[]{0x15, 0x03},
				Tuple.from(Versionstamp.complete(new byte[]{(byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03})), new byte[]{0x33, (byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03, 0x00, 0x00},
				Tuple.from(Versionstamp.complete(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}, 657)), new byte[]{0x33, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x02, (byte)0x91}
		);
		Tuple bigTuple = new Tuple();
		List<byte[]> serializedForms = new ArrayList<>();
		for(TupleSerialization serialization : serializations) {
			bigTuple = bigTuple.addAll(serialization.tuple);
			serializedForms.add(serialization.serialization);
		}
		serializations.add(new TupleSerialization(bigTuple, ByteArrayUtil.join(null, serializedForms)));

		for(TupleSerialization serialization : serializations) {
			System.out.println("Packing " + serialization.tuple + " (expecting: " + ByteArrayUtil.printable(serialization.serialization) + ")");
			if(serialization.tuple.getPackedSize() != serialization.serialization.length) {
				throw new RuntimeException("Tuple " + serialization.tuple + " packed size " + serialization.tuple.getPackedSize() + " does not match expected packed size " + serialization.serialization.length);
			}
			if(!Arrays.equals(serialization.tuple.pack(), serialization.serialization)) {
				throw new RuntimeException("Tuple " + serialization.tuple + " has serialization " + ByteArrayUtil.printable(serialization.tuple.pack()) +
						" which does not match expected serialization " + ByteArrayUtil.printable(serialization.serialization));
			}
			if(!Objects.equals(serialization.tuple, Tuple.fromItems(Tuple.fromBytes(serialization.serialization).getItems()))) {
				throw new RuntimeException("Tuple " + serialization.tuple + " does not match deserialization " + Tuple.fromBytes(serialization.serialization) +
						" which comes from serialization " + ByteArrayUtil.printable(serialization.serialization));
			}
		}
		System.out.println("All tuples had matching serializations");
	}

	private static void malformedStrings() {
		// Malformed when packing
		List<String> strings = Arrays.asList(
				"\ud83d", // high surrogate without low (end of string)
				"\ud83da", // high surrogate without low (not end of string)
				"\ud83d\ud8ed", // two high surrogates
				"\udd25", // low surrogate without low (start of string)
				"\udd26\udd6f", // two low surrogates
				"a\udd25", // low surrogate without high (not start of string)
				"a\udd25\udd6e", // two low surrogates (not start of string)
				"a\udd25\udd6f", // two low surrogates (not start of string)
				"\ud33d\udd25\udd25" // high surrogate followed by two low surrogates
		);

		// Verify that it won't be packed
		for(String s : strings) {
			Tuple t = Tuple.from(s);
			try {
				t.getPackedSize();
				throw new RuntimeException("able to get packed size of malformed string " + ByteArrayUtil.printable(s.getBytes()));
			}
			catch (IllegalArgumentException e) {
				// eat
			}
			try {
				t.pack();
				throw new RuntimeException("able to pack malformed string " + ByteArrayUtil.printable(s.getBytes()));
			}
			catch(IllegalArgumentException e) {
				// eat
			}
			try {
				// Modify the memoized packed size to match what it would be if naively packed.
				// This checks to make sure the validation logic invoked right before packing works,
				// but getting that code path to execute means modifying the tuple's internal state, hence
				// the reflection.
				Field f = Tuple.class.getDeclaredField("memoizedPackedSize");
				AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
					if(!f.isAccessible()) {
						f.setAccessible(true);
					}
					f.setInt(t, 2 + s.getBytes("UTF-8").length);
					return null;
				});
				t.pack();
				throw new RuntimeException("able to pack malformed string");
			}
			catch(NoSuchFieldException | PrivilegedActionException e) {
				throw new RuntimeException("reflection chicanery failed", e);
			}
			catch(IllegalArgumentException e) {
				// eat
			}
		}
	}

	private static void comparisons() {
		List<Tuple> tuples = Arrays.asList(
				Tuple.from(0L),
				Tuple.from(BigInteger.ZERO),
				Tuple.from(1L),
				Tuple.from(BigInteger.ONE),
				Tuple.from(-1L),
				Tuple.from(BigInteger.ONE.negate()),
				Tuple.from(Long.MAX_VALUE),
				Tuple.from(Long.MIN_VALUE),
				Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE)),
				Tuple.from(BigInteger.valueOf(Long.MIN_VALUE).shiftLeft(1)),
				Tuple.from(-0.0f),
				Tuple.from(0.0f),
				Tuple.from(-0.0),
				Tuple.from(0.0),
				Tuple.from(Float.NEGATIVE_INFINITY),
				Tuple.from(Double.NEGATIVE_INFINITY),
				Tuple.from(Float.NaN),
				Tuple.from(Double.NaN),
				Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) + 1)),
				Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) + 1)),
				Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) + 2)),
				Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) + 2)),
				Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) ^ Integer.MIN_VALUE)),
				Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) ^ Long.MIN_VALUE)),
				Tuple.from(Float.intBitsToFloat(Float.floatToIntBits(Float.NaN) ^ Integer.MIN_VALUE + 1)),
				Tuple.from(Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) ^ Long.MIN_VALUE + 1)),
				Tuple.from(Float.POSITIVE_INFINITY),
				Tuple.from(Double.POSITIVE_INFINITY),
				Tuple.from((Object)new byte[0]),
				Tuple.from((Object)new byte[]{0x00}),
				Tuple.from((Object)new byte[]{0x00, FF}),
				Tuple.from((Object)new byte[]{0x7f}),
				Tuple.from((Object)new byte[]{(byte)0x80}),
				Tuple.from(null, new byte[0]),
				Tuple.from(null, new byte[]{0x00}),
				Tuple.from(null, new byte[]{0x00, FF}),
				Tuple.from(null, new byte[]{0x7f}),
				Tuple.from(null, new byte[]{(byte)0x80}),
				Tuple.from(Tuple.from(null, new byte[0])),
				Tuple.from(Tuple.from(null, new byte[]{0x00})),
				Tuple.from(Tuple.from(null, new byte[]{0x00, FF})),
				Tuple.from(Tuple.from(null, new byte[]{0x7f})),
				Tuple.from(Tuple.from(null, new byte[]{(byte)0x80})),
				Tuple.from("a"),
				Tuple.from("\u03bc\u03ac\u03b8\u03b7\u03bc\u03b1"),
				Tuple.from("\u03bc\u03b1\u0301\u03b8\u03b7\u03bc\u03b1"),
				Tuple.from("\u4e2d\u6587"),
				Tuple.from("\u4e2d\u570B"),
				Tuple.from("\ud83d\udd25"),
				Tuple.from("\ud83e\udd6f"),
				Tuple.from("a\ud83d\udd25"),
				Tuple.from("\ufb49"),
				Tuple.from("\ud83d\udd25\ufb49"),
				Tuple.from(new UUID(-1, 0)),
				Tuple.from(new UUID(-1, -1)),
				Tuple.from(new UUID(1, -1)),
				Tuple.from(new UUID(1, 1)),
				Tuple.from(false),
				Tuple.from(true),
				Tuple.from(Arrays.asList(0, 1, 2)),
				Tuple.from(Arrays.asList(0, 1), "hello"),
				Tuple.from(Arrays.asList(0, 1), "help"),
				Tuple.from(Versionstamp.complete(new byte[]{0x0a, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03})),
				Tuple.from(Versionstamp.complete(new byte[]{(byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03})),
				Tuple.from(Versionstamp.complete(new byte[]{(byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03}, 1)),
				Tuple.from(Versionstamp.complete(new byte[]{(byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03}, 0xa101)),
				Tuple.from(Versionstamp.complete(new byte[]{(byte)0xaa, (byte)0xbb, (byte)0xcc, (byte)0xdd, (byte)0xee, FF, 0x00, 0x01, 0x02, 0x03}, 65535))

		);

		for(Tuple t1 : tuples) {
			for(Tuple t2 : tuples) {
				System.out.println("Comparing " + t1 + " and " + t2);
				// Copy the items over to new tuples to avoid having them use the memoized packed representations
				Tuple t1copy = Tuple.fromList(t1.getItems());
				Tuple t2copy = Tuple.fromList(t2.getItems());
				int semanticComparison = t1copy.compareTo(t2copy);
				int byteComparison = ByteArrayUtil.compareUnsigned(t1.pack(), t2.pack());
				if(Integer.signum(semanticComparison) != Integer.signum(byteComparison)) {
					throw new RuntimeException("Tuple t1 and t2 comparison mismatched: semantic = " + semanticComparison + " while byte order = " + byteComparison);
				}
				int implicitByteComparison = t1.compareTo(t2);
				if(Integer.signum(semanticComparison) != Integer.signum(implicitByteComparison)) {
					throw new RuntimeException("Tuple t1 and t2 comparison mismatched: semantic = " + semanticComparison + " while implicit byte order = " + implicitByteComparison);
				}
			}
		}
	}

	private static void emptyTuple() {
		Tuple t = new Tuple();
		if(!t.isEmpty()) {
			throw new RuntimeException("empty tuple is not empty");
		}
		if(t.getPackedSize() != 0) {
			throw new RuntimeException("empty tuple packed size is not 0");
		}
		if(t.pack().length != 0) {
			throw new RuntimeException("empty tuple is not packed to the empty byte string");
		}
	}

	private static void addMethods() {
		List<Tuple> baseTuples = Arrays.asList(
				new Tuple(),
				Tuple.from(),
				Tuple.from((Object)null),
				Tuple.from("prefix"),
				Tuple.from("prefix", null),
				Tuple.from(new UUID(100, 1000)),
				Tuple.from(Versionstamp.incomplete(1)),
				Tuple.from(Tuple.from(Versionstamp.incomplete(2))),
				Tuple.from(Collections.singletonList(Versionstamp.incomplete(3)))
		);
		List<Object> toAdd = Arrays.asList(
				null,
				1066L,
				BigInteger.valueOf(1066),
				-3.14f,
				2.71828,
				new byte[]{0x01, 0x02, 0x03},
				new byte[]{0x01, 0x00, 0x02, 0x00, 0x03},
				"hello there",
				"hell\0 there",
				"\ud83d\udd25",
				"\ufb14",
				false,
				true,
				Float.NaN,
				Float.intBitsToFloat(Integer.MAX_VALUE),
				Double.NaN,
				Double.longBitsToDouble(Long.MAX_VALUE),
				Versionstamp.complete(new byte[]{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, 100),
				Versionstamp.incomplete(4),
				new UUID(-1, 1),
				Tuple.from((Object)null),
				Tuple.from("suffix", "tuple"),
				Tuple.from("s\0ffix", "tuple"),
				Arrays.asList("suffix", "tuple"),
				Arrays.asList("suffix", null, "tuple"),
				Tuple.from("suffix", null, "tuple"),
				Tuple.from("suffix", Versionstamp.incomplete(4), "tuple"),
				Arrays.asList("suffix", Arrays.asList("inner", Versionstamp.incomplete(5), "tuple"), "tuple")
		);

		for(Tuple baseTuple : baseTuples) {
			for(Object newItem : toAdd) {
				int baseSize = baseTuple.size();
				Tuple freshTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(newItem)));
				if(freshTuple.size() != baseSize + 1) {
					throw new RuntimeException("freshTuple size was not one larger than base size");
				}
				Tuple withObjectAdded = baseTuple.addObject(newItem);
				if(withObjectAdded.size() != baseSize + 1) {
					throw new RuntimeException("withObjectAdded size was not one larger than the base size");
				}
				// Use the appropriate "add" overload.
				Tuple withValueAdded;
				if(newItem == null) {
					withValueAdded = baseTuple.addObject(null);
				}
				else if(newItem instanceof byte[]) {
					withValueAdded = baseTuple.add((byte[])newItem);
				}
				else if(newItem instanceof String) {
					withValueAdded = baseTuple.add((String)newItem);
				}
				else if(newItem instanceof Long) {
					withValueAdded = baseTuple.add((Long)newItem);
				}
				else if(newItem instanceof BigInteger) {
					withValueAdded = baseTuple.add((BigInteger)newItem);
				}
				else if(newItem instanceof Float) {
					withValueAdded = baseTuple.add((Float)newItem);
				}
				else if(newItem instanceof Double) {
					withValueAdded = baseTuple.add((Double)newItem);
				}
				else if(newItem instanceof Boolean) {
					withValueAdded = baseTuple.add((Boolean)newItem);
				}
				else if(newItem instanceof UUID) {
					withValueAdded = baseTuple.add((UUID)newItem);
				}
				else if(newItem instanceof Versionstamp) {
					withValueAdded = baseTuple.add((Versionstamp)newItem);
				}
				else if(newItem instanceof List<?>) {
					withValueAdded = baseTuple.add((List<?>)newItem);
				}
				else if(newItem instanceof Tuple) {
					withValueAdded = baseTuple.add((Tuple)newItem);
				}
				else {
					throw new RuntimeException("unknown type for tuple serialization " + newItem.getClass());
				}
				// Use Tuple.addAll, which has optimizations if both tuples have been packed already
				// Getting their hash codes memoizes the packed representation.
				Tuple newItemTuple = Tuple.from(newItem);
				baseTuple.hashCode();
				newItemTuple.hashCode();
				Tuple withTupleAddedAll = baseTuple.addAll(newItemTuple);
				Tuple withListAddedAll = baseTuple.addAll(Collections.singletonList(newItem));
				List<Tuple> allTuples = Arrays.asList(freshTuple, withObjectAdded, withValueAdded, withTupleAddedAll, withListAddedAll);

				int basePlusNewSize = baseTuple.getPackedSize() + Tuple.from(newItem).getPackedSize();
				int freshTuplePackedSize = freshTuple.getPackedSize();
				int withObjectAddedPackedSize = withObjectAdded.getPackedSize();
				int withValueAddedPackedSize = withValueAdded.getPackedSize();
				int withTupleAddedAllPackedSize = withTupleAddedAll.getPackedSize();
				int withListAddAllPackedSize = withListAddedAll.getPackedSize();
				if(basePlusNewSize != freshTuplePackedSize || basePlusNewSize != withObjectAddedPackedSize ||
						basePlusNewSize != withValueAddedPackedSize || basePlusNewSize != withTupleAddedAllPackedSize ||
						basePlusNewSize != withListAddAllPackedSize) {
					throw new RuntimeException("packed sizes not equivalent");
				}
				byte[] concatPacked;
				byte[] prefixPacked;
				byte[] freshPacked;
				byte[] objectAddedPacked;
				byte[] valueAddedPacked;
				byte[] tupleAddedAllPacked;
				byte[] listAddedAllPacked;
				if(!baseTuple.hasIncompleteVersionstamp() && !Tuple.from(newItem).hasIncompleteVersionstamp()) {
					concatPacked = ByteArrayUtil.join(baseTuple.pack(), Tuple.from(newItem).pack());
					prefixPacked = Tuple.from(newItem).pack(baseTuple.pack());
					freshPacked = freshTuple.pack();
					objectAddedPacked = withObjectAdded.pack();
					valueAddedPacked = withValueAdded.pack();
					tupleAddedAllPacked = withTupleAddedAll.pack();
					listAddedAllPacked = withListAddedAll.pack();

					for(Tuple t : allTuples) {
						try {
							t.packWithVersionstamp();
							throw new RuntimeException("able to pack tuple without incomplete versionstamp using packWithVersionstamp");
						}
						catch(IllegalArgumentException e) {
							// eat
						}
					}
				}
				else if(!baseTuple.hasIncompleteVersionstamp() && Tuple.from(newItem).hasIncompleteVersionstamp()) {
					concatPacked = newItemTuple.packWithVersionstamp(baseTuple.pack());
					try {
						prefixPacked = Tuple.from(newItem).packWithVersionstamp(baseTuple.pack());
					}
					catch(NullPointerException e) {
						prefixPacked = Tuple.from(newItem).packWithVersionstamp(baseTuple.pack());
					}
					freshPacked = freshTuple.packWithVersionstamp();
					objectAddedPacked = withObjectAdded.packWithVersionstamp();
					valueAddedPacked = withValueAdded.packWithVersionstamp();
					tupleAddedAllPacked = withTupleAddedAll.packWithVersionstamp();
					listAddedAllPacked = withListAddedAll.packWithVersionstamp();

					for(Tuple t : allTuples) {
						try {
							t.pack();
							throw new RuntimeException("able to pack tuple with incomplete versionstamp");
						}
						catch(IllegalArgumentException e) {
							// eat
						}
					}
				}
				else if(baseTuple.hasIncompleteVersionstamp() && !Tuple.from(newItem).hasIncompleteVersionstamp()) {
					concatPacked = baseTuple.addAll(Tuple.from(newItem)).packWithVersionstamp();
					prefixPacked = baseTuple.addObject(newItem).packWithVersionstamp();
					freshPacked = freshTuple.packWithVersionstamp();
					objectAddedPacked = withObjectAdded.packWithVersionstamp();
					valueAddedPacked = withValueAdded.packWithVersionstamp();
					tupleAddedAllPacked = withTupleAddedAll.packWithVersionstamp();
					listAddedAllPacked = withListAddedAll.packWithVersionstamp();

					for(Tuple t : allTuples) {
						try {
							t.pack();
							throw new RuntimeException("able to pack tuple with incomplete versionstamp");
						}
						catch(IllegalArgumentException e) {
							// eat
						}
					}
				}
				else {
					for(Tuple t : allTuples) {
						try {
							t.pack();
							throw new RuntimeException("able to pack tuple with two versionstamps using pack");
						}
						catch(IllegalArgumentException e) {
							// eat
						}
						try {
							t.packWithVersionstamp();
							throw new RuntimeException("able to pack tuple with two versionstamps using packWithVersionstamp");
						}
						catch(IllegalArgumentException e) {
							// eat
						}
						try {
							t.hashCode();
							throw new RuntimeException("able to get hash code of tuple with two versionstamps");
						}
						catch(IllegalArgumentException e) {
							// eat
						}
					}
					concatPacked = null;
					prefixPacked = null;
					freshPacked = null;
					objectAddedPacked = null;
					valueAddedPacked = null;
					tupleAddedAllPacked = null;
					listAddedAllPacked = null;
				}
				if(!Arrays.equals(concatPacked, freshPacked) ||
						!Arrays.equals(freshPacked, prefixPacked) ||
						!Arrays.equals(freshPacked, objectAddedPacked) ||
						!Arrays.equals(freshPacked, valueAddedPacked) ||
						!Arrays.equals(freshPacked, tupleAddedAllPacked) ||
						!Arrays.equals(freshPacked, listAddedAllPacked)) {
					throw new RuntimeException("packed values are not concatenation of original packings");
				}
				if(freshPacked != null && freshPacked.length != basePlusNewSize) {
					throw new RuntimeException("packed length did not match expectation");
				}
				if(freshPacked != null) {
					if(freshTuple.hashCode() != Arrays.hashCode(freshPacked)) {
						throw new IllegalArgumentException("hash code does not match fresh packed");
					}
					for(Tuple t : allTuples) {
						if(t.hashCode() != freshTuple.hashCode()) {
							throw new IllegalArgumentException("hash code mismatch");
						}
						if(Tuple.fromItems(t.getItems()).hashCode() != freshTuple.hashCode()) {
							throw new IllegalArgumentException("hash code mismatch after re-compute");
						}
					}
				}
			}
		}
	}

	private static void incompleteVersionstamps() {
		if(FDB.instance().getAPIVersion() < 520) {
			throw new IllegalStateException("cannot run test with API version " + FDB.instance().getAPIVersion());
		}
		// This is a tricky case where there are two tuples with identical representations but different semantics.
		byte[] arr = new byte[0x0100fe];
		Arrays.fill(arr, (byte)0x7f); // The actual value doesn't matter, but it can't be zero.
		Tuple t1 = Tuple.from(arr, Versionstamp.complete(new byte[]{FF, FF, FF, FF, FF, FF, FF, FF, FF, FF}), new byte[]{0x01, 0x01});
		Tuple t2 = Tuple.from(arr, Versionstamp.incomplete());
		if(t1.equals(t2)) {
			throw new RuntimeException("tuples " + t1 + " and " + t2 + " compared equal");
		}
		byte[] bytes1 = t1.pack();
		byte[] bytes2 = t2.packWithVersionstamp();
		if(!Arrays.equals(bytes1, bytes2)) {
			throw new RuntimeException("tuples " + t1 + " and " + t2 + " did not have matching representations");
		}
		if(t1.equals(t2)) {
			throw new RuntimeException("tuples " + t1 + " and " + t2 + " compared equal with memoized packed representations");
		}

		// Make sure position information adjustment works.
		Tuple t3 = Tuple.from(Versionstamp.incomplete(1));
		if(t3.getPackedSize() != 1 + Versionstamp.LENGTH + Integer.BYTES) {
			throw new RuntimeException("incomplete versionstamp has incorrect packed size " + t3.getPackedSize());
		}
		byte[] bytes3 = t3.packWithVersionstamp();
		if(ByteBuffer.wrap(bytes3, bytes3.length - Integer.BYTES, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt() != 1) {
			throw new RuntimeException("incomplete versionstamp has incorrect position");
		}
		if(!Tuple.fromBytes(bytes3, 0, bytes3.length - Integer.BYTES).equals(Tuple.from(Versionstamp.incomplete(1)))) {
			throw new RuntimeException("unpacked bytes did not match");
		}
		Subspace subspace = new Subspace(Tuple.from("prefix"));
		byte[] bytes4 = subspace.packWithVersionstamp(t3);
		if(ByteBuffer.wrap(bytes4, bytes4.length - Integer.BYTES, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt() != 1 + subspace.getKey().length) {
			throw new RuntimeException("incomplete versionstamp has incorrect position with prefix");
		}
		if(!Tuple.fromBytes(bytes4, 0, bytes4.length - Integer.BYTES).equals(Tuple.from("prefix", Versionstamp.incomplete(1)))) {
			throw new RuntimeException("unpacked bytes with subspace did not match");
		}
		try {
			// At this point, the representation is cached, so an easy bug would be to have it return the already serialized value
			t3.pack();
			throw new RuntimeException("was able to pack versionstamp with incomplete versionstamp");
		} catch(IllegalArgumentException e) {
			// eat
		}

		// Tuples with two incomplete versionstamps somewhere.
		List<Tuple> twoIncompleteList = Arrays.asList(
				Tuple.from(Versionstamp.incomplete(1), Versionstamp.incomplete(2)),
				Tuple.from(Tuple.from(Versionstamp.incomplete(3)), Tuple.from(Versionstamp.incomplete(4))),
				new Tuple().add(Versionstamp.incomplete()).add(Versionstamp.incomplete()),
				new Tuple().add(Versionstamp.incomplete()).add(3L).add(Versionstamp.incomplete()),
				Tuple.from(Tuple.from(Versionstamp.incomplete()), "dummy_string").add(Tuple.from(Versionstamp.incomplete())),
				Tuple.from(Arrays.asList(Versionstamp.incomplete(), "dummy_string")).add(Tuple.from(Versionstamp.incomplete())),
				Tuple.from(Tuple.from(Versionstamp.incomplete()), "dummy_string").add(Collections.singletonList(Versionstamp.incomplete()))
		);
		for(Tuple t : twoIncompleteList) {
			if(!t.hasIncompleteVersionstamp()) {
				throw new RuntimeException("tuple doesn't think it has incomplete versionstamp");
			}
			if(t.getPackedSize() < 2 * (1 + Versionstamp.LENGTH + Integer.BYTES)) {
				throw new RuntimeException("tuple packed size " + t.getPackedSize() + " is smaller than expected");
			}
			try {
				t.pack();
				throw new RuntimeException("no error thrown when packing any incomplete versionstamps");
			}
			catch(IllegalArgumentException e) {
				// eat
			}
			try {
				t.packWithVersionstamp();
				throw new RuntimeException("no error thrown when packing with versionstamp with two incompletes");
			}
			catch(IllegalArgumentException e) {
				// eat
			}
		}
	}

	// Assumes API version < 520
	private static void incompleteVersionstamps300() {
		if(FDB.instance().getAPIVersion() >= 520) {
			throw new IllegalStateException("cannot run test with API version " + FDB.instance().getAPIVersion());
		}
		Tuple t1 = Tuple.from(Versionstamp.complete(new byte[]{FF, FF, FF, FF, FF, FF, FF, FF, FF, FF}), new byte[]{});
		Tuple t2 = Tuple.from(Versionstamp.incomplete());
		if(t1.equals(t2)) {
			throw new RuntimeException("tuples " + t1 + " and " + t2 + " compared equal");
		}
		byte[] bytes1 = t1.pack();
		byte[] bytes2 = t2.packWithVersionstamp();
		if(!Arrays.equals(bytes1, bytes2)) {
			throw new RuntimeException("tuples " + t1 + " and " + t2 + " did not have matching representations");
		}
		if(t1.equals(t2)) {
			throw new RuntimeException("tuples " + t1 + " and " + t2 + " compared equal with memoized packed representations");
		}

		// Make sure position information adjustment works.
		Tuple t3 = Tuple.from(Versionstamp.incomplete(1));
		if(t3.getPackedSize() != 1 + Versionstamp.LENGTH + Short.BYTES) {
			throw new RuntimeException("incomplete versionstamp has incorrect packed size " + t3.getPackedSize());
		}
		byte[] bytes3 = t3.packWithVersionstamp();
		if(ByteBuffer.wrap(bytes3, bytes3.length - Short.BYTES, Short.BYTES).order(ByteOrder.LITTLE_ENDIAN).getShort() != 1) {
			throw new RuntimeException("incomplete versionstamp has incorrect position");
		}
		if(!Tuple.fromBytes(bytes3, 0, bytes3.length - Short.BYTES).equals(Tuple.from(Versionstamp.incomplete(1)))) {
			throw new RuntimeException("unpacked bytes did not match");
		}
		Subspace subspace = new Subspace(Tuple.from("prefix"));
		byte[] bytes4 = subspace.packWithVersionstamp(t3);
		if(ByteBuffer.wrap(bytes4, bytes4.length - Short.BYTES, Short.BYTES).order(ByteOrder.LITTLE_ENDIAN).getShort() != 1 + subspace.getKey().length) {
			throw new RuntimeException("incomplete versionstamp has incorrect position with prefix");
		}
		if(!Tuple.fromBytes(bytes4, 0, bytes4.length - Short.BYTES).equals(Tuple.from("prefix", Versionstamp.incomplete(1)))) {
			throw new RuntimeException("unpacked bytes with subspace did not match");
		}

		// Make sure an offset > 0xFFFF throws an error.
		Tuple t4 = Tuple.from(Versionstamp.incomplete(2));
		byte[] bytes5 = t4.packWithVersionstamp(); // Get bytes memoized.
		if(ByteBuffer.wrap(bytes5, bytes5.length - Short.BYTES, Short.BYTES).order(ByteOrder.LITTLE_ENDIAN).getShort() != 1) {
			throw new RuntimeException("incomplete versionstamp has incorrect position with prefix");
		}
		byte[] bytes6 = t4.packWithVersionstamp(new byte[0xfffe]); // Offset is 0xffff
		if(!Arrays.equals(Arrays.copyOfRange(bytes5, 0, 1 + Versionstamp.LENGTH), Arrays.copyOfRange(bytes6, 0xfffe, 0xffff + Versionstamp.LENGTH))) {
			throw new RuntimeException("area before versionstamp offset did not match");
		}
		if((ByteBuffer.wrap(bytes6, bytes6.length - Short.BYTES, Short.BYTES).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xffff) != 0xffff) {
			throw new RuntimeException("incomplete versionstamp has incorrect position with prefix");
		}
		try {
			t4.packWithVersionstamp(new byte[0xffff]); // Offset is 0x10000
			throw new RuntimeException("able to pack versionstamp with offset that is too large");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		// Same as before, but packed representation is not memoized.
		try {
			Tuple.from(Versionstamp.incomplete(3)).packWithVersionstamp(new byte[0xffff]); // Offset is 0x10000
			throw new RuntimeException("able to pack versionstamp with offset that is too large");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
	}

	private static void malformedBytes() {
		List<byte[]> malformedSequences = Arrays.asList(
				new byte[]{0x01, (byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde}, // no termination character for byte array
				new byte[]{0x01, (byte)0xde, (byte)0xad, 0x00, FF, (byte)0xc0, (byte)0xde}, // no termination character but null in middle
				new byte[]{0x02, 'h', 'e', 'l', 'l', 'o'}, // no termination character for string
				new byte[]{0x02, 'h', 'e', 'l', 0x00, FF, 'l', 'o'}, // no termination character but null in the middle
				new byte[]{0x02, 'u', 't', 'f', 0x08, (byte)0x80, 0x00}, // invalid utf-8 code point start character
				new byte[]{0x02, 'u', 't', 'f', 0x08, (byte)0xc0, 0x01, 0x00}, // invalid utf-8 code point second character
				new byte[]{0x02, 'u', 't', 'f', 0x10, (byte)0xed, (byte)0xa0, (byte)0xbd, (byte)0x00}, // invalid utf-8 (corresponds to high surrogate \ud83d)
				new byte[]{0x02, 'u', 't', 'f', 0x10, (byte)0xed, (byte)0xb4, (byte)0xa5, (byte)0x00}, // invalid utf-8 (corresponds to low surrogate \udd25)
				new byte[]{0x02, 'u', 't', 'f', 0x10, (byte)0xed, (byte)0xa0, (byte)0xbd, (byte)0xed, (byte)0xb4, (byte)0xa5, (byte)0x00}, // invalid utf-8 (corresponds to \ud83d\udd25 which *is* valid utf-16, but not encoded like that)
				new byte[]{0x05, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00}, // no termination character for nested tuple
				new byte[]{0x05, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00, FF, 0x02, 't', 'h', 'e', 'r', 'e', 0x00}, // no termination character for nested tuple but null in the middle
				new byte[]{0x16, 0x01}, // integer truncation
				new byte[]{0x12, 0x01}, // integer truncation
				new byte[]{0x1d, 0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, // integer truncation
				new byte[]{0x0b, 0x09 ^ FF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, // integer truncation
				new byte[]{0x20, 0x01, 0x02, 0x03}, // float truncation
				new byte[]{0x21, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, // double truncation
				new byte[]{0x30, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e}, // UUID truncation
				new byte[]{0x33, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b}, // versionstamp truncation
				new byte[]{FF} // unknown start code
		);
		for(byte[] sequence : malformedSequences) {
			try {
				Tuple t = Tuple.fromBytes(sequence);
				throw new RuntimeException("Able to unpack " + ByteArrayUtil.printable(sequence) + " into " + t);
			}
			catch(IllegalArgumentException e) {
				System.out.println("Error for " + ByteArrayUtil.printable(sequence) + ": " + e.getMessage());
			}
		}

		// Perfectly good byte sequences, but using the offset and length to remove terminal bytes
		List<byte[]> wellFormedSequences = Arrays.asList(
				Tuple.from((Object)new byte[]{0x01, 0x02}).pack(),
				Tuple.from("hello").pack(),
				Tuple.from("hell\0").pack(),
				Tuple.from(1066L).pack(),
				Tuple.from(-1066L).pack(),
				Tuple.from(BigInteger.ONE.shiftLeft(Long.SIZE + 1)).pack(),
				Tuple.from(BigInteger.ONE.shiftLeft(Long.SIZE + 1).negate()).pack(),
				Tuple.from(-3.14f).pack(),
				Tuple.from(2.71828).pack(),
				Tuple.from(new UUID(1066L, 1415L)).pack(),
				Tuple.from(Versionstamp.fromBytes(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c})).pack()
		);
		for(byte[] sequence : wellFormedSequences) {
			try {
				Tuple t = Tuple.fromBytes(sequence, 0, sequence.length - 1);
				throw new RuntimeException("Able to unpack " + ByteArrayUtil.printable(sequence) + " into " + t + " without last character");
			}
			catch(IllegalArgumentException e) {
				System.out.println("Error for " + ByteArrayUtil.printable(sequence) + ": " + e.getMessage());
			}
		}
	}

	private static void offsetsAndLengths() {
		List<Tuple> tuples = Arrays.asList(
				new Tuple(),
				Tuple.from((Object)null),
				Tuple.from(null, new byte[]{0x10, 0x66}),
				Tuple.from("dummy_string"),
				Tuple.from(1066L)
		);
		Tuple allTuples = tuples.stream().reduce(new Tuple(), Tuple::addAll);
		byte[] allTupleBytes = allTuples.pack();

		// Unpack each tuple individually using their lengths
		int offset = 0;
		for(Tuple t : tuples) {
			int length = t.getPackedSize();
			Tuple unpacked = Tuple.fromBytes(allTupleBytes, offset, length);
			if(!unpacked.equals(t)) {
				throw new RuntimeException("unpacked tuple " + unpacked + " does not match serialized tuple " + t);
			}
			offset += length;
		}

		// Unpack successive pairs of tuples.
		offset = 0;
		for(int i = 0; i < tuples.size() - 1; i++) {
			Tuple combinedTuple = tuples.get(i).addAll(tuples.get(i + 1));
			Tuple unpacked = Tuple.fromBytes(allTupleBytes, offset, combinedTuple.getPackedSize());
			if(!unpacked.equals(combinedTuple)) {
				throw new RuntimeException("unpacked tuple " + unpacked + " does not match combined tuple " + combinedTuple);
			}
			offset += tuples.get(i).getPackedSize();
		}

		// Allow an offset to equal the length of the array, but essentially only a zero-length is allowed there.
		Tuple emptyAtEndTuple = Tuple.fromBytes(allTupleBytes, allTupleBytes.length, 0);
		if(!emptyAtEndTuple.isEmpty()) {
			throw new RuntimeException("tuple with no bytes is not empty");
		}

		try {
			Tuple.fromBytes(allTupleBytes, -1, 4);
			throw new RuntimeException("able to give negative offset to fromBytes");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		try {
			Tuple.fromBytes(allTupleBytes, allTupleBytes.length + 1, 4);
			throw new RuntimeException("able to give offset larger than array to fromBytes");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		try {
			Tuple.fromBytes(allTupleBytes, 0, -1);
			throw new RuntimeException("able to give negative length to fromBytes");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		try {
			Tuple.fromBytes(allTupleBytes, 0, allTupleBytes.length + 1);
			throw new RuntimeException("able to give length larger than array to fromBytes");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		try {
			Tuple.fromBytes(allTupleBytes, allTupleBytes.length / 2, allTupleBytes.length / 2 + 2);
			throw new RuntimeException("able to exceed array length in fromBytes");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
	}

	private static void intoBuffer() {
		Tuple t = Tuple.from("hello", 3.14f, "world");
		ByteBuffer buffer = ByteBuffer.allocate("hello".length() + 2 + Float.BYTES + 1 + "world".length() + 2);
		t.packInto(buffer);
		if(!Arrays.equals(t.pack(), buffer.array())) {
			throw new RuntimeException("buffer and tuple do not match");
		}

		buffer = ByteBuffer.allocate(t.getPackedSize() + 2);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		t.packInto(buffer);
		if(!Arrays.equals(ByteArrayUtil.join(t.pack(), new byte[]{0x00, 0x00}), buffer.array())) {
			throw new RuntimeException("buffer and tuple do not match");
		}
		if(!buffer.order().equals(ByteOrder.LITTLE_ENDIAN)) {
			throw new RuntimeException("byte order changed");
		}

		buffer = ByteBuffer.allocate(t.getPackedSize() + 2);
		buffer.put((byte)0x01).put((byte)0x02);
		t.packInto(buffer);
		if(!Arrays.equals(t.pack(new byte[]{0x01, 0x02}), buffer.array())) {
			throw new RuntimeException("buffer and tuple do not match");
		}

		buffer = ByteBuffer.allocate(t.getPackedSize() - 1);
		try {
			t.packInto(buffer);
			throw new RuntimeException("able to pack into buffer that was too small");
		}
		catch(BufferOverflowException e) {
			// eat
		}

		Tuple tCopy = Tuple.fromItems(t.getItems()); // remove memoized stuff
		buffer = ByteBuffer.allocate(t.getPackedSize() - 1);
		try {
			tCopy.packInto(buffer);
			throw new RuntimeException("able to pack into buffer that was too small");
		}
		catch(BufferOverflowException e) {
			// eat
		}

		Tuple tWithIncomplete = Tuple.from(Versionstamp.incomplete(3));
		buffer = ByteBuffer.allocate(tWithIncomplete.getPackedSize());
		try {
			tWithIncomplete.packInto(buffer);
			throw new RuntimeException("able to pack incomplete versionstamp into buffer");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		if(buffer.arrayOffset() != 0) {
			throw new RuntimeException("offset changed after unsuccessful pack with incomplete versionstamp");
		}
	}

	// These should be in ArrayUtilTest, but those can't be run at the moment, so here they go.
	private static void replaceTests() {
		List<byte[]> arrays = Arrays.asList(
				new byte[]{0x01, 0x02, 0x01, 0x02}, new byte[]{0x01, 0x02}, new byte[]{0x03, 0x04}, new byte[]{0x03, 0x04, 0x03, 0x04},
				new byte[]{0x01, 0x02, 0x01, 0x02}, new byte[]{0x01, 0x02}, new byte[]{0x03}, new byte[]{0x03, 0x03},
				new byte[]{0x01, 0x02, 0x01, 0x02}, new byte[]{0x01, 0x02}, new byte[]{0x03, 0x04, 0x05}, new byte[]{0x03, 0x04, 0x05, 0x03, 0x04, 0x05},
				new byte[]{0x00, 0x01, 0x02, 0x00, 0x01, 0x02, 0x00}, new byte[]{0x01, 0x02}, new byte[]{0x03, 0x04, 0x05}, new byte[]{0x00, 0x03, 0x04, 0x05, 0x00, 0x03, 0x04, 0x05, 0x00},
				new byte[]{0x01, 0x01, 0x01, 0x01}, new byte[]{0x01, 0x02}, new byte[]{0x03, 0x04}, new byte[]{0x01, 0x01, 0x01, 0x01},
				new byte[]{0x01, 0x01, 0x01, 0x01}, new byte[]{0x01, 0x02}, new byte[]{0x03}, new byte[]{0x01, 0x01, 0x01, 0x01},
				new byte[]{0x01, 0x01, 0x01, 0x01}, new byte[]{0x01, 0x02}, new byte[]{0x03, 0x04, 0x05}, new byte[]{0x01, 0x01, 0x01, 0x01},
				new byte[]{0x01, 0x01, 0x01, 0x01, 0x01}, new byte[]{0x01, 0x01}, new byte[]{0x03, 0x04, 0x05}, new byte[]{0x03, 0x04, 0x05, 0x03, 0x04, 0x05, 0x01},
				new byte[]{0x01, 0x01, 0x01, 0x01, 0x01}, new byte[]{0x01, 0x01}, new byte[]{0x03, 0x04}, new byte[]{0x03, 0x04, 0x03, 0x04, 0x01},
				new byte[]{0x01, 0x01, 0x01, 0x01, 0x01}, new byte[]{0x01, 0x01}, new byte[]{0x03}, new byte[]{0x03, 0x03, 0x01},
				new byte[]{0x01, 0x02, 0x01, 0x02}, new byte[]{0x01, 0x02}, null, new byte[0],
				new byte[]{0x01, 0x02, 0x01, 0x02}, new byte[]{0x01, 0x02}, new byte[0], new byte[0],
				new byte[]{0x01, 0x02, 0x01, 0x02}, null, new byte[]{0x04}, new byte[]{0x01, 0x02, 0x01, 0x02},
				new byte[]{0x01, 0x02, 0x01, 0x02}, new byte[0], new byte[]{0x04}, new byte[]{0x01, 0x02, 0x01, 0x02},
				null, new byte[]{0x01, 0x02}, new byte[]{0x04}, null
		);
		for(int i = 0; i < arrays.size(); i += 4) {
			byte[] src = arrays.get(i);
			byte[] pattern = arrays.get(i + 1);
			byte[] replacement = arrays.get(i + 2);
			byte[] expectedResults = arrays.get(i + 3);
			byte[] results = ByteArrayUtil.replace(src, pattern, replacement);
			if(!Arrays.equals(results, expectedResults)) {
				throw new RuntimeException("results " + ByteArrayUtil.printable(results) + " did not match expected results " +
						ByteArrayUtil.printable(expectedResults) + " when replacing " + ByteArrayUtil.printable(pattern) +
						" with " + ByteArrayUtil.printable(replacement) + " in " + ByteArrayUtil.printable(src));
			}
			if(src != null && src == results) {
				throw new RuntimeException("src and results array are pointer-equal when replacing " + ByteArrayUtil.printable(pattern) +
						" with " + ByteArrayUtil.printable(replacement) + " in " + ByteArrayUtil.printable(src));
			}
		}

		try {
			ByteArrayUtil.replace(null, 0, 1, new byte[]{0x00}, new byte[]{0x00, FF});
			throw new RuntimeException("able to replace null bytes");
		}
		catch(NullPointerException e) {
			// eat
		}
		try {
			ByteArrayUtil.replace(new byte[]{0x00, 0x01}, -1, 2, new byte[]{0x00}, new byte[]{0x00, FF});
			throw new RuntimeException("able to use negative offset");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		try {
			ByteArrayUtil.replace(new byte[]{0x00, 0x01}, 3, 2, new byte[]{0x00}, new byte[]{0x00, FF});
			throw new RuntimeException("able to use offset after end of array");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		try {
			ByteArrayUtil.replace(new byte[]{0x00, 0x01}, 1, -1, new byte[]{0x00}, new byte[]{0x00, FF});
			throw new RuntimeException("able to use negative length");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
		try {
			ByteArrayUtil.replace(new byte[]{0x00, 0x01}, 1, 2, new byte[]{0x00}, new byte[]{0x00, FF});
			throw new RuntimeException("able to give length that exceeds end of the array");
		}
		catch(IllegalArgumentException e) {
			// eat
		}
	}

	private static void runTests(final int reps, TransactionContext db) {
		System.out.println("Running tests...");
		long start = System.currentTimeMillis();
		try {
			db.run(tr -> {
				Tuple t = new Tuple();
				t.add(100230045000L);
				t.add("Hello!");
				t.add("foo".getBytes());

				/*for(Map.Entry<byte[], byte[]> e : tr.getRange("vcount".getBytes(), "zz".getBytes())) {
					System.out.println("K: " + new String(e.getKey()) + ", V: " + new String(e.getValue()));
				}*/
				return null;
			});
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();

		double seconds = (end - start) / 1000.0;
		System.out.println(" Transactions:    " + reps);
		System.out.println(" Total Time:      " + seconds);
		System.out.println(" Gets+Sets / sec: " + reps / seconds);

		System.exit(0);
	}

	private TupleTest() {}
}
