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

import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TupleTest {
	private static final byte FF = (byte)0xff;

	public static void main(String[] args) throws InterruptedException {
		final int reps = 1000;
		try {
			// FDB fdb = FDB.selectAPIVersion(610);
			serializedForms();
			/*
			try(Database db = fdb.open()) {
				runTests(reps, db);
			}
			*/
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
				Tuple.from(Double.longBitsToDouble(~0L)), new byte[]{0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		);

		for(TupleSerialization serialization : serializations) {
			System.out.println("Packing " + serialization.tuple + " (expecting: " + ByteArrayUtil.printable(serialization.serialization) + ")");
			if(!Arrays.equals(serialization.tuple.pack(), serialization.serialization)) {
				throw new RuntimeException("Tuple " + serialization.tuple + " has serialization " + ByteArrayUtil.printable(serialization.tuple.pack()) +
						" which does not match expected serialization " + ByteArrayUtil.printable(serialization.serialization));
			}
			if(!Objects.equals(serialization.tuple, Tuple.fromBytes(serialization.serialization))) {
				throw new RuntimeException("Tuple " + serialization.tuple + " does not match deserialization " + Tuple.fromBytes(serialization.serialization) +
						" which comes from serialization " + ByteArrayUtil.printable(serialization.serialization));
			}
		}
		System.out.println("All tuples had matching serializations");
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
