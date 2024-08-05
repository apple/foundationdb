/*
 * ArrayUtilTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * @author Ben
 *
 */
class ArrayUtilTest {

	/**
	 * Test method for {@link ByteArrayUtil#join(byte[], java.util.List)}.
	 */
	@Test
	void testJoinByteArrayListOfbyte() {
		byte[] a = new byte[] { 'a', 'b', 'c' };
		byte[] b = new byte[] { 'd', 'e', 'f' };

		List<byte[]> parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		byte[] result = new byte[] { 'a', 'b', 'c', 'z', 'd', 'e', 'f', 'z' };
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] { 'z' }, parts));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		result = new byte[] { 'z', 'a', 'b', 'c', 'z', 'd', 'e', 'f' };
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] { 'z' }, parts));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		result = new byte[] { 'z', 'z', 'a', 'b', 'c', 'z', 'd', 'e', 'f' };
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] { 'z' }, parts));

		parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(b);
		result = new byte[] { 'a', 'b', 'c', 'z', 'z', 'z', 'd', 'e', 'f' };
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] { 'z' }, parts));

		parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] { 'b' });
		result = new byte[] { 'a', 'b', 'c', 'z', 'd', 'e', 'f', 'z', 'b' };
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] { 'z' }, parts));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		result = new byte[] { 'z', 'z' };
		assertArrayEquals(result, ByteArrayUtil.join(new byte[] { 'z' }, parts));

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
	void testJoinByteArrayArray() {
		byte[] a = new byte[] { 'a', 'b', 'c' };
		byte[] b = new byte[] { 'd', 'e', 'f' };

		List<byte[]> parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		byte[] result = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f' };
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][] {})));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		result = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f' };
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][] {})));

		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] { 'b' });
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		parts.add(new byte[] {});
		result = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'b' };
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][] {})));

		parts = new ArrayList<byte[]>();
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] { 'b' });
		result = new byte[] { 'a', 'b', 'c', 'd', 'e', 'f', 'b' };
		assertArrayEquals(result, ByteArrayUtil.join(parts.toArray(new byte[][] {})));

		// Self-referential, with conversion to array
		parts = new ArrayList<byte[]>();
		parts.add(new byte[] {});
		parts.add(a);
		parts.add(b);
		parts.add(new byte[] {});
		assertArrayEquals(ByteArrayUtil.join(a, b), ByteArrayUtil.join(parts.toArray(new byte[][] {})));

		// Test exception on null elements
		boolean isError = false;
		try {
			ByteArrayUtil.join(a, b, null);
		} catch (Exception e) {
			isError = true;
		} finally {
			assertTrue(isError);
		}
	}

	/**
	 * Test method for {@link ByteArrayUtil#regionEquals(byte[], int, byte[])}.
	 */
	@Test
	void testRegionEquals() {
		byte[] src = new byte[] { 'a', (byte)12, (byte)255, 'n', 'm', 'z', 'k' };
		assertTrue(ByteArrayUtil.regionEquals(src, 3, new byte[] { 'n', 'm' }));

		assertFalse(ByteArrayUtil.regionEquals(src, 2, new byte[] { 'n', 'm' }));

		assertTrue(ByteArrayUtil.regionEquals(null, 0, null));

		assertFalse(ByteArrayUtil.regionEquals(src, 0, null));
	}

	/**
	 * Test method for {@link ByteArrayUtil#replace(byte[], byte[], byte[])}.
	 */
	@Test
	void testReplace() {
		byte[] a = new byte[] { 'a', 'b', 'c' };
		byte[] b = new byte[] { 'd', 'e', 'f' };

		byte[] src = ByteArrayUtil.join(a, b, a, b);
		byte[] result = new byte[] { 'z', 'd', 'e', 'f', 'z', 'd', 'e', 'f' };
		assertArrayEquals(result, ByteArrayUtil.replace(src, a, new byte[] { 'z' }));

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
	void testSplit() {
		byte[] a = new byte[] { 'a', 'b', 'c' };
		byte[] b = new byte[] { 'd', 'e', 'f' };

		byte[] src = ByteArrayUtil.join(a, b, a, b, a);
		List<byte[]> parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 3);
		for (byte[] p : parts) {
			assertArrayEquals(a, p);
		}

		src = ByteArrayUtil.join(b, a, b, a, b, a);
		parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 4);
		int counter = 0;
		for (byte[] p : parts) {
			if (counter++ == 0)
				assertArrayEquals(new byte[] {}, p);
			else
				assertArrayEquals(a, p);
		}

		src = ByteArrayUtil.join(a, b, a, b, a, b);
		parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 4);
		counter = 0;
		for (byte[] p : parts) {
			if (counter++ < 3)
				assertArrayEquals(a, p);
			else
				assertArrayEquals(new byte[] {}, p);
		}

		// Multiple ending delimiters
		src = ByteArrayUtil.join(a, b, a, b, a, b, b, b);
		parts = ByteArrayUtil.split(src, b);
		assertEquals(parts.size(), 6);
		counter = 0;
		for (byte[] p : parts) {
			if (counter++ < 3)
				assertArrayEquals(a, p);
			else
				assertArrayEquals(new byte[] {}, p);
		}
	}

	/**
	 * Test method for
	 * {@link ByteArrayUtil#bisectLeft(java.math.BigInteger[], java.math.BigInteger)}.
	 */
	@Test
	@Disabled("not implemented")
	void testBisectLeft() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#compareUnsigned(byte[], byte[])}.
	 */
	@Test
	@Disabled("not implemented")
	void testCompare() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#findNext(byte[], byte, int)}.
	 */
	@Test
	@Disabled("not implemented")
	void testFindNext() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for
	 * {@link ByteArrayUtil#findTerminator(byte[], byte, byte, int)}.
	 */
	@Test
	@Disabled("not implemented")
	void testFindTerminator() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#copyOfRange(byte[], int, int)}.
	 */
	@Test
	@Disabled("not implemented")
	void testCopyOfRange() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#strinc(byte[])}.
	 */
	@Test
	@Disabled("not implemented")
	void testStrinc() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link ByteArrayUtil#printable(byte[])}.
	 */
	@Test
	@Disabled("not implemented")
	void testPrintable() {
		fail("Not yet implemented");
	}

	@Test
	void cannotReplaceNullBytes() throws Exception {
		Assertions.assertThrows(NullPointerException.class, () -> {
			ByteArrayUtil.replace(null, 0, 1, new byte[] { 0x00 }, new byte[] { 0x00, (byte)0xFF });
		});
	}

	@Test
	void cannotReplaceWithNegativeOffset() throws Exception {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			ByteArrayUtil.replace(new byte[] { 0x00, 0x01 }, -1, 2, new byte[] { 0x00 },
			                      new byte[] { 0x00, (byte)0xFF });
		});
	}

	@Test
	void cannotReplaceWithNegativeLength() throws Exception {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			ByteArrayUtil.replace(new byte[] { 0x00, 0x01 }, 1, -1, new byte[] { 0x00 },
			                      new byte[] { 0x00, (byte)0xFF });
		});
	}

	@Test
	void cannotReplaceWithOffsetAfterEndOfArray() throws Exception {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			ByteArrayUtil.replace(new byte[] { 0x00, 0x01 }, 3, 2, new byte[] { 0x00 },
			                      new byte[] { 0x00, (byte)0xFF });
		});
	}

	@Test
	void cannotReplaceWithLengthAfterEndOfArray() throws Exception {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			ByteArrayUtil.replace(new byte[] { 0x00, 0x01 }, 1, 2, new byte[] { 0x00 },
			                      new byte[] { 0x00, (byte)0xFF });
		});
	}

	@Test
	void replaceWorks() throws Exception {
		List<byte[]> arrays = Arrays.asList(
		    new byte[] { 0x01, 0x02, 0x01, 0x02 }, new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04 },
		    new byte[] { 0x03, 0x04, 0x03, 0x04 }, new byte[] { 0x01, 0x02, 0x01, 0x02 }, new byte[] { 0x01, 0x02 },
		    new byte[] { 0x03 }, new byte[] { 0x03, 0x03 }, new byte[] { 0x01, 0x02, 0x01, 0x02 },
		    new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04, 0x05 },
		    new byte[] { 0x03, 0x04, 0x05, 0x03, 0x04, 0x05 }, new byte[] { 0x00, 0x01, 0x02, 0x00, 0x01, 0x02, 0x00 },
		    new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04, 0x05 },
		    new byte[] { 0x00, 0x03, 0x04, 0x05, 0x00, 0x03, 0x04, 0x05, 0x00 }, new byte[] { 0x01, 0x01, 0x01, 0x01 },
		    new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04 }, new byte[] { 0x01, 0x01, 0x01, 0x01 },
		    new byte[] { 0x01, 0x01, 0x01, 0x01 }, new byte[] { 0x01, 0x02 }, new byte[] { 0x03 },
		    new byte[] { 0x01, 0x01, 0x01, 0x01 }, new byte[] { 0x01, 0x01, 0x01, 0x01 }, new byte[] { 0x01, 0x02 },
		    new byte[] { 0x03, 0x04, 0x05 }, new byte[] { 0x01, 0x01, 0x01, 0x01 },
		    new byte[] { 0x01, 0x01, 0x01, 0x01, 0x01 }, new byte[] { 0x01, 0x01 }, new byte[] { 0x03, 0x04, 0x05 },
		    new byte[] { 0x03, 0x04, 0x05, 0x03, 0x04, 0x05, 0x01 }, new byte[] { 0x01, 0x01, 0x01, 0x01, 0x01 },
		    new byte[] { 0x01, 0x01 }, new byte[] { 0x03, 0x04 }, new byte[] { 0x03, 0x04, 0x03, 0x04, 0x01 },
		    new byte[] { 0x01, 0x01, 0x01, 0x01, 0x01 }, new byte[] { 0x01, 0x01 }, new byte[] { 0x03 },
		    new byte[] { 0x03, 0x03, 0x01 }, new byte[] { 0x01, 0x02, 0x01, 0x02 }, new byte[] { 0x01, 0x02 }, null,
		    new byte[0], new byte[] { 0x01, 0x02, 0x01, 0x02 }, new byte[] { 0x01, 0x02 }, new byte[0], new byte[0],
		    new byte[] { 0x01, 0x02, 0x01, 0x02 }, null, new byte[] { 0x04 }, new byte[] { 0x01, 0x02, 0x01, 0x02 },
		    new byte[] { 0x01, 0x02, 0x01, 0x02 }, new byte[0], new byte[] { 0x04 },
		    new byte[] { 0x01, 0x02, 0x01, 0x02 }, null, new byte[] { 0x01, 0x02 }, new byte[] { 0x04 }, null);
		for (int i = 0; i < arrays.size(); i += 4) {
			byte[] src = arrays.get(i);
			byte[] pattern = arrays.get(i + 1);
			byte[] replacement = arrays.get(i + 2);
			byte[] expectedResults = arrays.get(i + 3);
			byte[] results = ByteArrayUtil.replace(src, pattern, replacement);
			String errorMsg = String.format(
			    "results <%s> did not match expected results <%s> when replaceing <%s> with <%s> in <%s>",
			    ByteArrayUtil.printable(results), ByteArrayUtil.printable(expectedResults),
			    ByteArrayUtil.printable(pattern), ByteArrayUtil.printable(replacement), ByteArrayUtil.printable(src));

			Assertions.assertArrayEquals(expectedResults, results, errorMsg);
			if (src != null) {
				Assertions.assertTrue(
				    src != results,
				    String.format("src and results array are pointer-equal when replacing <%s> with <%s> in <%s>",
				                  ByteArrayUtil.printable(pattern), ByteArrayUtil.printable(replacement),
				                  ByteArrayUtil.printable(src)));
			}
		}
	}
}
