package com.apple.foundationdb.tuple;

import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.*;
import java.util.List;

import static org.junit.Assert.*;

public class TupleTest {

	@Test
	public void testByte() {
		List<Object> list = new ArrayList<>();
		byte[] b1 =  new byte[] {0};
		byte[] b2 =  new byte[] {9, 5, 23, 42, 64, 3};
		byte[] b3 =  new byte[] {1, 2, 7, 4, 7, 2, 12, 56, 23, 1};
		list.add(b1);
		list.add(b2);
		list.add(b3);
		byte[] res = TupleUtil.pack(list, null);
		assertArrayEquals(b1, (byte[]) ByteArrayUtil.getObject(res, 0));
		assertArrayEquals(b2, (byte[]) ByteArrayUtil.getObject(res, 1));
		assertArrayEquals(b3, (byte[]) ByteArrayUtil.getObject(res, 2));
	}

	@Test
	public void testString() {
		List<Object> list = new ArrayList<>();
		String str1 = "\u0000Sam";
		String str2 = "S\u0000a\u0000m\u0000";
		String str3 = "skhdfur3dsf54x,.23";
		list.add(str1);
		list.add(str2);
		list.add(str3);
		byte[] res = TupleUtil.pack(list, null);
		assertEquals(str1, ByteArrayUtil.getObject(res, 0));
		assertEquals(str2, ByteArrayUtil.getObject(res, 1));
		assertEquals(str3, ByteArrayUtil.getObject(res, 2));
	}

	@Test
	public void testDouble() {
		List<Object> list = new ArrayList<>();
		byte[] res = null;
		double delta = 1e-15;
		for (double d = 1.24; d < Double.MAX_VALUE; d *= 2.516) {
			for (int j = 0; j < 2; ++j) {
				list.add(d);
				res = TupleUtil.pack(list, null);
				assertEquals(d, (double) ByteArrayUtil.getObject(res, 0), delta);
				list.clear();
				d *= -1;
			}
		}
	}

	@Test
	public void testBoolean() {
		List<Object> list = new ArrayList<>();
		list.add(true);
		list.add(false);
		byte[] res = TupleUtil.pack(list, null);
		assertEquals(true, ByteArrayUtil.getObject(res, 0));
		assertEquals(false, ByteArrayUtil.getObject(res, 1));
	}

	@Test
	public void testUUID() {
		List<Object> list = new ArrayList<>();
		byte[] res = null;
		for (int i = 0; i < 100; ++i) {
			UUID id = UUID.randomUUID();
			list.add(id);
			res = TupleUtil.pack(list, null);
			assertEquals(id, ByteArrayUtil.getObject(res, 0));
			list.clear();
		}
	}

	@Test
	public void testLong() {
		List<Object> list = new ArrayList<>();
		byte[] res = null;
		for (long l = 1; l > 0; l *= 2) {
			for (int j = 0; j < 2; ++j) {
				list.add(l);
				res = TupleUtil.pack(list, null);
				assertEquals(l, (long) ByteArrayUtil.getObject(res, 0));
				list.clear();
				l *= -1;
			}
		}
	}

	@Test
	public void testBigInteger() {
		Random rand = new Random();
		List<Object> list = new ArrayList<>();
		byte[] res = null;
		BigInteger big = new BigInteger(String.valueOf(Long.MAX_VALUE) + 1);
		for (int i = 1; i < 100; ++i) {
			big.multiply(new BigInteger(String.valueOf(rand.nextInt(10))));
			list.add(big);
			res = TupleUtil.pack(list, null);
			assertEquals(big, ByteArrayUtil.getObject(res, 0));
			list.clear();
			big = big.negate();
			list.add(big);
			res = TupleUtil.pack(list, null);
			assertEquals(big, ByteArrayUtil.getObject(res, 0));
			list.clear();
		}
	}

	@Test
	public void testVersionStamp() {
		byte[] b1 =  new byte[] {1, 2, 7, 4, 7, 2, 12, 56, 23, 1};
		byte[] b2 =  new byte[] {43, 65, 22, 3, 46, 34, 12, 32, 17, 32};
		Versionstamp v1 = Versionstamp.complete(b1, 3);
		Versionstamp v2 = Versionstamp.complete(b2);
		List<Object> list = new ArrayList<>();
		list.add(v1);
		list.add(v2);
		byte[] res = TupleUtil.pack(list, null);
		assertEquals(v1, ByteArrayUtil.getObject(res, 0));
		assertEquals(v2, ByteArrayUtil.getObject(res, 1));
		v1 = Versionstamp.complete(b1, 2);
		v2 = Versionstamp.complete(b2, 1);
		assertNotEquals(v1, ByteArrayUtil.getObject(res, 0));
		assertNotEquals(v2, ByteArrayUtil.getObject(res, 1));
	}

	@Test
	public void testList() {
		List<Object> objects = Arrays.asList(13L, "weffs", false, UUID.randomUUID());
		List<Object> list = new ArrayList<>();
		list.add(objects);
		byte[] res = TupleUtil.pack(list, null);
		assertEquals(objects, ByteArrayUtil.getObject(res, 0));
	}

	@Test
	public void testGetObject() {
		List<Object> objects = Arrays.asList(13L, "weffs", false, UUID.randomUUID());
		List<Object> list = new ArrayList<>();
		list.add(13);
		list.add("String");
		byte[] res = TupleUtil.pack(list, null);
		int[] count = new int[1];
		int[] offset = new int[1];
		assertEquals(13L, (long) ByteArrayUtil.getObject(res, 0));
		assertEquals("String", ByteArrayUtil.getObject(res, 1));
		ByteArrayUtil.getObject(res, 0, 0, count, offset);
		assertEquals(2, count[0]);
		assertEquals("String", ByteArrayUtil.getObject(res, 0, offset[0]));
	}

	private int compare(byte[] left, byte[] right) {
		for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
			int a = (left[i] & 0xff);
			int b = (right[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return left.length - right.length;
	}

	@Test
	@Ignore
	public void testTupleBench() {
		Tuple metrics = Tuple.from("h").add(new byte[]{1});
		byte[] metricId = new byte[]{1, 2, 3};
		byte[] hostB = new byte[]{1, 2, 3};
		for (int i = 0; i < 3; i++) {
			{
				// Check
				Long start = System.currentTimeMillis();
				int size = metrics.size();
				List<Object> list = new ArrayList<>(size + 3);
				for (int j = 0; j < size; j++) {
					list.add(metrics.get(j));
				}
				list.add(metricId);
				list.add(hostB);
				list.add(start);
				byte[] reference = metrics.add(metricId).add(hostB).add(start).pack();
				assertEquals(0, compare(reference, TupleUtil.pack(list, null)));

				List<Object> items = metrics.getItems();
				items.add(Long.MAX_VALUE);
				assertEquals(0, compare(metrics.add(Long.MAX_VALUE).pack(),
						TupleUtil.pack(items,null)));

				byte[] refMin = metrics.add(Long.MIN_VALUE).pack();
				items =  metrics.getItems();
				items.add(Long.MIN_VALUE);
				assertEquals(0, compare(refMin, TupleUtil.pack(items, null)));
			}
			{
				Long start = System.currentTimeMillis();
				int total = 0;
				while (System.currentTimeMillis() - start < 5000) {
					metrics.add(metricId).add(hostB).add(start).pack();
					total++;
				}
				System.out.println("Tuple: " + total);
			}
			{
				Long start = System.currentTimeMillis();
				int total = 0;
				while (System.currentTimeMillis() - start < 5000) {
					TupleUtil.pack(Arrays.asList(metrics, metricId, hostB, start), null);
					total++;
				}
				System.out.println("TupleWriter.pack2: " + total);
			}
		}
	}
}
