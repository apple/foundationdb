/*
 * ArrayUtilTests.java
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

/**
 * @author Ben
 *
 */
public class ArrayUtilTests {

	/**
	 * Test method for {@link ByteArrayUtil#join(byte[], java.util.List)}.
	 */
	@Test
	public void testJoinByteArrayListOfbyte() {
		byte[] a = new byte[] {'a', 'b', 'c'};
		byte[] b = new byte[] {'d', 'e', 'f'};

		List<byte[]> parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		byte[] result = new byte[] {'a', 'b', 'c', 'z', 'd', 'e', 'f', 'z'};
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] {'z'}, parts));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		result = new byte[] {'z', 'a', 'b', 'c', 'z', 'd', 'e', 'f'};
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] {'z'}, parts));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		result = new byte[] {'z', 'z', 'a', 'b', 'c', 'z', 'd', 'e', 'f'};
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] {'z'}, parts));

		parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(b);
		result = new byte[] {'a', 'b', 'c', 'z', 'z', 'z', 'd', 'e', 'f'};
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] {'z'}, parts));

		parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {'b'});
		result = new byte[] {'a', 'b', 'c', 'z', 'd', 'e', 'f', 'z', 'b'};
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] {'z'}, parts));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		result = new byte[] {'z', 'z'};
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] {'z'}, parts));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		result = new byte[] {};
		assertArrayEquals(result, ByteArrayUtil.join(null, parts));
	}

	/**
	 * Test method for {@link ByteArrayUtil#join(byte[][])}.
	 */
	@Test
	public void testJoinByteArrayArray() {
		byte[] a = new byte[] {'a', 'b', 'c'};
		byte[] b = new byte[] {'d', 'e', 'f'};

		List<byte[]> parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		byte[] result = new byte[] {'a', 'b', 'c', 'd', 'e', 'f'};
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][]{})));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		result = new byte[] {'a', 'b', 'c', 'd', 'e', 'f'};
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][]{})));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {'b'});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		result = new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 'b'};
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][]{})));

		parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {'b'});
		result = new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 'b'};
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][]{})));

		// Self-referential, with conversion to array
		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		assertArrayEquals(ByteArrayUtil.join(a, b), ByteArrayUtil.join(parts.toArray(new byte[][]{})));

		// Test exception on null elements
		boolean isError = false;
		try {
			ByteArrayUtil.join(a, b, null);
		} catch(Exception e) {
			isError = true;
		} finally {
			assertTrue(isError);
		}
	}

	/**
	 * Test method for {@link ByteArrayUtil#regionEquals(byte[], int, byte[])}.
	 */
	@Test
	public void testRegionEquals() {
		byte[] src = new byte[] {'a', (byte)12, (byte)255, 'n', 'm', 'z', 'k'};
		assertTrue(ByteArrayUtil.regionEquals(src, 3, new byte[] { 'n', 'm' }));

		assertFalse(ByteArrayUtil.regionEquals(src, 2, new byte[] { 'n', 'm' }));

		assertTrue(ByteArrayUtil.regionEquals(null, 0, null));

		assertFalse(ByteArrayUtil.regionEquals(src, 0, null));
	}

	/**
	 * Test method for {@link ByteArrayUtil#replace(byte[], byte[], byte[])}.
	 */
	@Test
	public void testReplace() {
		byte[] a = new byte[] {'a', 'b', 'c'};
		byte[] b = new byte[] {'d', 'e', 'f'};

		byte[] src = ByteArrayUtil.join(a, b, a, b);
		byte[] result = new byte[] {'z', 'd', 'e', 'f', 'z', 'd', 'e', 'f'};
		assertArrayEquals(result, ByteArrayUtil.replace(src, a, new byte[] {'z'}));

		src = ByteArrayUtil.join(a, b, a, b);
		assertArrayEquals(ByteArrayUtil.join(b, b), ByteArrayUtil.replace(src, a, new byte[] {}));

		src = ByteArrayUtil.join(a, b, a, b);
		assertArrayEquals(ByteArrayUtil.join(a, a), ByteArrayUtil.replace(src, b, new byte[] {}));

		src = ByteArrayUtil.join(a, a, a);
		assertArrayEquals(new byte[] {}, ByteArrayUtil.replace(src, a, new byte[] {}));
	}

	/**
	 * Test method for {@link ByteArrayUtil#split(byte[], byte[])}.
	 */
	@Test
	public void testSplit() {
		byte[] a = new byte[] {'a', 'b', 'c'};
		byte[] b = new byte[] {'d', 'e', 'f'};

		byte[] src = ByteArrayUtil.join(a, b, a, b, a);
		List<byte[]> parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 3);
		for(byte[] p : parts) {
			assertArrayEquals(a, p);
		}

		src = ByteArrayUtil.join(b, a, b, a, b, a);
		parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 4);
		int counter = 0;
		for(byte[] p : parts) {
			if(counter++ == 0)
				assertArrayEquals(new byte[]{}, p);
			else
				assertArrayEquals(a, p);
		}

		src = ByteArrayUtil.join(a, b, a, b, a, b);
		parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 4);
		counter = 0;
		for(byte[] p : parts) {
			if(counter++ < 3)
				assertArrayEquals(a, p);
			else
				assertArrayEquals(new byte[]{}, p);
		}

		// Multiple ending delimiters
		src = ByteArrayUtil.join(a, b, a, b, a, b, b, b);
		parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 6);
		counter = 0;
		for(byte[] p : parts) {
			if(counter++ < 3)
				assertArrayEquals(a, p);
			else
				assertArrayEquals(new byte[]{}, p);
		}
	}

	/**
	 * Test method for {@link ByteArrayUtil#bisectLeft(java.math.BigInteger[], java.math.BigInteger)}.
	 */
	@Test @Ignore
	public void testBisectLeft() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#compareUnsigned(byte[], byte[])}.
	 */
	@Test @Ignore
	public void testCompare() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#findNext(byte[], byte, int)}.
	 */
	@Test @Ignore
	public void testFindNext() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#findTerminator(byte[], byte, byte, int)}.
	 */
	@Test @Ignore
	public void testFindTerminator() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#copyOfRange(byte[], int, int)}.
	 */
	@Test @Ignore
	public void testCopyOfRange() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#strinc(byte[])}.
	 */
	@Test @Ignore
	public void testStrinc() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#printable(byte[])}.
	 */
	@Test @Ignore
	public void testPrintable() {
		fail("Not yet implemented");
	}

	private static final int SAMPLE_COUNT = 1000000;
	private static final int SAMPLE_MAX_SIZE = 2048;
	private List<byte[]> unsafe;
	private List<byte[]> java;
	@Before
	public void init() {
		unsafe = new ArrayList(SAMPLE_COUNT);
		java = new ArrayList(SAMPLE_COUNT);
		Random random = new Random();
		for (int i = 0; i <= SAMPLE_COUNT; i++) {
			byte[] addition = new byte[random.nextInt(SAMPLE_MAX_SIZE)];
			random.nextBytes(addition);
			unsafe.add(addition);
			java.add(addition);
		}
	}

	@Test
	public void testComparatorSort() {
		Collections.sort(unsafe, FastByteComparisons.lexicographicalComparerUnsafeImpl());
		Collections.sort(java, FastByteComparisons.lexicographicalComparerJavaImpl());
		Assert.assertTrue(unsafe.equals(java));
	}

	@Test
	public void testUnsafeComparison() {
		for (int i =0; i< SAMPLE_COUNT; i++) {
			Assert.assertEquals(FastByteComparisons.lexicographicalComparerUnsafeImpl().compare(unsafe.get(i), java.get(i)), 0);
		}
	}

	@Test
	public void testJavaComparison() {
		for (int i =0; i< SAMPLE_COUNT; i++) {
			Assert.assertEquals(FastByteComparisons.lexicographicalComparerJavaImpl().compare(unsafe.get(i), java.get(i)), 0);
		}
	}

	@Test
	public void testUnsafeComparisonWithOffet() {
		for (int i =0; i< SAMPLE_COUNT; i++) {
			if (unsafe.get(i).length > 5)
				Assert.assertEquals(FastByteComparisons.lexicographicalComparerUnsafeImpl().compareTo(unsafe.get(i), 4, unsafe.get(i).length - 4,  java.get(i), 4, java.get(i).length - 4), 0);
		}
	}

	@Test
	public void testJavaComparisonWithOffset() {
		for (int i =0; i< SAMPLE_COUNT; i++) {
			if (unsafe.get(i).length > 5)
				Assert.assertEquals(FastByteComparisons.lexicographicalComparerJavaImpl().compareTo(unsafe.get(i), 4, unsafe.get(i).length - 4,  java.get(i), 4, java.get(i).length - 4), 0);
		}
	}

}
