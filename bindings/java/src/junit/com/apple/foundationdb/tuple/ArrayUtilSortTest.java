/*
 * ArrayUtilSortTests.java
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests relating to sorting
 */
class ArrayUtilSortTest {

	private static final int SAMPLE_COUNT = 100000;
	private static final int SAMPLE_MAX_SIZE = 2048;
	private static List<byte[]> unsafe;
	private static List<byte[]> java;

	@BeforeAll
	static void initTestClass() {
		unsafe = new ArrayList<>(SAMPLE_COUNT);
		java = new ArrayList<>(SAMPLE_COUNT);
		Random random = new Random();
		for (int i = 0; i <= SAMPLE_COUNT; i++) {
			byte[] addition = new byte[random.nextInt(SAMPLE_MAX_SIZE)];
			random.nextBytes(addition);
			unsafe.add(addition);
			java.add(addition);
		}
	}

	@Test
	void testUnsafeSortSorts() throws Exception {
		/*
		 * We want to test whether or not our comparator works, but that's hard to do
		 * with a byte[] comparator because there isn't a canonical comparator to work
		 * with, so any direct comparison would be written here and just have a
		 * potential for breaking. To avoid that, we just compare our two different
		 * implementations and make sure that they agree
		 */
		// sort it using unsafe logic
		Collections.sort(unsafe, FastByteComparisons.lexicographicalComparerUnsafeImpl());
		Collections.sort(java, FastByteComparisons.lexicographicalComparerJavaImpl());

		Assertions.assertEquals(java.size(), unsafe.size(), "unsafe and java comparators disagree");
		for (int i = 0; i < java.size(); i++) {
			Assertions.assertArrayEquals(java.get(i), unsafe.get(i), "[pos ]" + i + ": comparators disagree");
		}
	}

	@Test
	void testUnsafeComparison() {
		for (int i = 0; i < unsafe.size(); i++) {
			Assertions.assertEquals(
			    0, FastByteComparisons.lexicographicalComparerUnsafeImpl().compare(unsafe.get(i), java.get(i)));
		}
	}

	@Test
	void testJavaComparison() {
		for (int i = 0; i < unsafe.size(); i++) {
			Assertions.assertEquals(
			    0, FastByteComparisons.lexicographicalComparerJavaImpl().compare(unsafe.get(i), java.get(i)));
		}
	}

	@Test
	void testUnsafeComparisonWithOffset() {
		for (int i = 0; i < unsafe.size(); i++) {
			if (unsafe.get(i).length > 5)
				Assertions.assertEquals(
				    0, FastByteComparisons.lexicographicalComparerUnsafeImpl().compareTo(
				           unsafe.get(i), 4, unsafe.get(i).length - 4, java.get(i), 4, java.get(i).length - 4));
		}
	}

	@Test
	void testJavaComparisonWithOffset() {
		for (int i = 0; i < unsafe.size(); i++) {
			if (unsafe.get(i).length > 5)
				Assertions.assertEquals(
				    0, FastByteComparisons.lexicographicalComparerJavaImpl().compareTo(
				           unsafe.get(i), 4, unsafe.get(i).length - 4, java.get(i), 4, java.get(i).length - 4));
		}
	}
}
