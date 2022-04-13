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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

public class TupleTest {
	private static final byte FF = (byte)0xff;

	public static void main(String[] args) throws NoSuchFieldException {
		final int reps = 1000;
		try {
			FDB fdb = FDB.selectAPIVersion(720);
			try(Database db = fdb.open()) {
				runTests(reps, db);
			}
		} catch(Throwable t) {
			t.printStackTrace();
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
