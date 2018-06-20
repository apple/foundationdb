package com.apple.foundationdb.tuple;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// This test is used to calculate memory usage and performance differences.
// This test should be done manually comparing results before and after fix.
public class TupleMemoryPerformanceTest {
	private class Pair
	{
		double memory;
		double performance;
		Pair(double m, double p) {
			memory = m;
			performance = p;
		}
	}

	private static class Type {
		static final int DOUBLE = 0x01;
		static final int LONG = 0x02;
		static final int BIGINT = 0x04;
		static final int STRING = 0x08;
		static final int UUID = 0x10;
		//ALL should be at the end and include all types.
		static final int ALL = DOUBLE | LONG | BIGINT | STRING | UUID;
	}

	private static class Count {
		static final int DOUBLE = 200000;
		static final int LONG = 200000;
		static final int BIGINT = 200000;
		static final int STRING = 50000;
		static final int UUID = 200000;
	}

	@Test
	@Ignore
	public void testMemoryPerformance() {
		testMemoryPerformance(Type.DOUBLE);
		testMemoryPerformance(Type.LONG);
		testMemoryPerformance(Type.BIGINT);
		testMemoryPerformance(Type.STRING);
		testMemoryPerformance(Type.UUID);
		testMemoryPerformance(Type.ALL);
	}

	private void testMemoryPerformance(int type) {
		Pair p = new Pair(0, 0);
		long count = 0;
		int test_count = 20;
		for (int i = 0; i < test_count; ++i) {
			count = testMemoryPerformance(p, type);
		}
		System.out.println("Tested " + count + " " + getTypeName(type));
		System.out.println("used memory = " + p.memory / test_count + " Mb");
		System.out.println("performance = " + p.performance / test_count + " ms");
		System.out.println();
	}

	private List<Object> getTestingObjects(int type) {
		List<Object> list = new ArrayList<>();
		Random rand = new Random();
		if ((type & Type.DOUBLE) != 0) {
			for (int i = 0; i < Count.DOUBLE; ++i) {
					list.add(rand.nextDouble());
			}
		}
		if ((type & Type.LONG) != 0) {
			for (int i = 0; i < Count.LONG; ++i) {
				list.add(rand.nextLong());
			}
		}
		if ((type & Type.BIGINT) != 0) {
			for (int i = 0; i < Count.BIGINT; ++i) {
				list.add(getRandomBigInteger());
			}
		}
		if ((type & Type.STRING) != 0) {
			for (int i = 0; i < Count.STRING; ++i) {
				list.add(getRandomString());
			}
		}
		if ((type & Type.UUID) != 0) {
			for (int i = 0; i < Count.UUID; ++i) {
				list.add(UUID.randomUUID());
			}
		}
		return list;
	}

	private long testMemoryPerformance(Pair p, int type) {
		Runtime runtime = Runtime.getRuntime();
		List<Object> list = getTestingObjects(type);

		//WeakReferences are used to interrupt the execution
		//if garbage collector is called during memory usage calculation.
		WeakReference<Integer> wr1 = new WeakReference<>(new Integer(0));
		runtime.gc();
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e) {
			throw new AssertionError();
		}
		//To be sure that garbage collector is called.
		Integer r1 = wr1.get();
		assert r1 == null;

		WeakReference<Integer> wr2 = new WeakReference<>(new Integer(0));
		long usedMemory1 = runtime.totalMemory() - runtime.freeMemory();
		long start = System.currentTimeMillis();
		TupleUtil.pack(list, null);
		long end = System.currentTimeMillis();
		long usedMemory2 = runtime.totalMemory() - runtime.freeMemory();

		//To be sure that garbage collector isn't called during pack.
		Integer r2 = wr2.get();
		assert r2 != null;

		p.performance += (end - start);
		p.memory += ((double)(usedMemory2 - usedMemory1)) / 1000000;
		return list.size();
	}

	private String getRandomString() {
		Random rand = new Random();
		StringBuilder stringBuilder = new StringBuilder(110);
		int count = 10 + rand.nextInt(100);
		for (int i = 0; i < count; ++i) {
			stringBuilder.append((char)rand.nextInt(128));
		}
		return stringBuilder.toString();
	}

	private BigInteger getRandomBigInteger() {
		Random rand = new Random();
		StringBuilder stringBuilder = new StringBuilder(150);
		if (rand.nextInt(2) == 0)  {
			stringBuilder.append("-");
		}
		stringBuilder.append(1 + rand.nextInt(9));
		int count = 20 + rand.nextInt(80);
		for (int i = 0; i < count; ++i) {
			stringBuilder.append(rand.nextInt(10));
		}
		return new BigInteger(stringBuilder.toString());
	}

	private String getTypeName(int type) {
		switch(type) {
			case Type.DOUBLE:
				return "double";
			case Type.LONG:
				return "long";
			case Type.BIGINT:
				return "BigInteger";
			case Type.STRING:
				return "String";
			case Type.UUID:
				return "UUID";
			case Type.ALL:
				return "ALL";
			default:
				throw new AssertionError("Invalid type");
		}
	}
}
