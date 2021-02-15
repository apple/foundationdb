/*
 * TupleEncodingTest.java
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

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBLibraryRule;
import com.apple.foundationdb.subspace.Subspace;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests around packing, versionstamps, and assorted encoding-related stuff for
 * tuples.
 *
 * This class should be used when adding/modifying tests that involve the "packing" logic,
 * while {@link TupleSerializationTest}  should be used to test "serialization" behavior. Granted,
 * that distinction is pretty arbitrary and not really clear, but the main motivation for separating the two
 * classes is just to avoid having a single ludicrously large test file, so it's probably not the end of the world
 * if this rule isn't perfectly followed.
 */
class TuplePackingTest {
	private static final byte FF = (byte)0xff;
	@RegisterExtension static final FDBLibraryRule fdbLib = FDBLibraryRule.current();

	static final List<Tuple> baseTuples =
	    Arrays.asList(new Tuple(), Tuple.from(), Tuple.from((Object)null), Tuple.from("prefix"),
	                  Tuple.from("prefix", null), Tuple.from(new UUID(100, 1000)),
	                  Tuple.from(Versionstamp.incomplete(1)), Tuple.from(Tuple.from(Versionstamp.incomplete(2))),
	                  Tuple.from(Collections.singletonList(Versionstamp.incomplete(3))));

	static final List<Object> items = Arrays.asList(
	    null, 1066L, BigInteger.valueOf(1066), BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), -3.14f, 2.71828,
	    new byte[] { 0x01, 0x02, 0x03 }, new byte[] { 0x01, 0x00, 0x02, 0x00, 0x03 }, "hello there", "hell\0 there",
	    "\ud83d\udd25", "\ufb14", false, true, Float.NaN, Float.intBitsToFloat(Integer.MAX_VALUE), Double.NaN,
	    Double.longBitsToDouble(Long.MAX_VALUE),
	    Versionstamp.complete(new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09 }, 100),
	    Versionstamp.incomplete(4), new UUID(-1, 1), Tuple.from((Object)null), Tuple.from("suffix", "tuple"),
	    Tuple.from("s\0ffix", "tuple"), Arrays.asList("suffix", "tuple"), Arrays.asList("suffix", null, "tuple"),
	    Tuple.from("suffix", null, "tuple"), Tuple.from("suffix", Versionstamp.incomplete(4), "tuple"),
	    Arrays.asList("suffix", Arrays.asList("inner", Versionstamp.incomplete(5), "tuple"), "tuple"));

	static final Stream<Arguments> baseAddCartesianProduct() {
		return baseTuples.stream().flatMap((left) -> {
			Stream<Object> oStream = items.stream();
			return items.stream().map((right) -> { return Arguments.of(left, right); });
		});
	}

	static List<Tuple> twoIncomplete() {
		return Arrays.asList(Tuple.from(Versionstamp.incomplete(1), Versionstamp.incomplete(2)),
		                     Tuple.from(Tuple.from(Versionstamp.incomplete(3)), Tuple.from(Versionstamp.incomplete(4))),
		                     new Tuple().add(Versionstamp.incomplete()).add(Versionstamp.incomplete()),
		                     new Tuple().add(Versionstamp.incomplete()).add(3L).add(Versionstamp.incomplete()),
		                     Tuple.from(Tuple.from(Versionstamp.incomplete()), "dummy_string")
		                         .add(Tuple.from(Versionstamp.incomplete())),
		                     Tuple.from(Arrays.asList(Versionstamp.incomplete(), "dummy_string"))
		                         .add(Tuple.from(Versionstamp.incomplete())),
		                     Tuple.from(Tuple.from(Versionstamp.incomplete()), "dummy_string")
		                         .add(Collections.singletonList(Versionstamp.incomplete())));
	}

	static final List<byte[]> malformedSequences() {
		return Arrays.asList(
		    new byte[] { 0x01, (byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde }, // no termination
		                                                                         // character for
		                                                                         // byte array
		    new byte[] { 0x01, (byte)0xde, (byte)0xad, 0x00, FF, (byte)0xc0, (byte)0xde }, // no termination
		                                                                                   // character but null
		                                                                                   // in middle
		    new byte[] { 0x02, 'h', 'e', 'l', 'l', 'o' }, // no termination character for string
		    new byte[] { 0x02, 'h', 'e', 'l', 0x00, FF, 'l', 'o' }, // no termination character but null in the
		                                                            // middle
		    new byte[] { 0x02, 'u', 't', 'f', 0x08, (byte)0x80, 0x00 }, // invalid utf-8 code point start character
		    new byte[] { 0x02, 'u', 't', 'f', 0x08, (byte)0xc0, 0x01, 0x00 }, // invalid utf-8 code point second
		                                                                      // character
		    // invalid utf-8 (corresponds to high surrogate \ud83d)
		    new byte[] { 0x02, 'u', 't', 'f', 0x10, (byte)0xed, (byte)0xa0, (byte)0xbd, (byte)0x00 },
		    // invalid utf-8 (corresponds to low surrogate \udd25)
		    new byte[] { 0x02, 'u', 't', 'f', 0x10, (byte)0xed, (byte)0xb4, (byte)0xa5, (byte)0x00 },
		    // invalid utf-8 (corresponds to \ud83d\udd25 which *is* valid utf-16, but not
		    // encoded like that)
		    new byte[] { 0x02, 'u', 't', 'f', 0x10, (byte)0xed, (byte)0xa0, (byte)0xbd, (byte)0xed, (byte)0xb4,
		                 (byte)0xa5, (byte)0x00 },
		    new byte[] { 0x05, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00 }, // no termination character for nested tuple
		    // no termination character for nested tuple but null in the middle
		    new byte[] { 0x05, 0x02, 'h', 'e', 'l', 'l', 'o', 0x00, 0x00, FF, 0x02, 't', 'h', 'e', 'r', 'e', 0x00 },
		    new byte[] { 0x16, 0x01 }, // integer truncation
		    new byte[] { 0x12, 0x01 }, // integer truncation
		    new byte[] { 0x1d, 0x09, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }, // integer truncation
		    new byte[] { 0x0b, 0x09 ^ FF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 }, // integer truncation
		    new byte[] { 0x20, 0x01, 0x02, 0x03 }, // float truncation
		    new byte[] { 0x21, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07 }, // double truncation
		    // UUID truncation
		    new byte[] { 0x30, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e },
		    // versionstamp truncation
		    new byte[] { 0x33, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b },
		    new byte[] { FF } // unknown
		                      // start
		                      // code
		);
	}

	static final List<byte[]> wellFormedSequences() {
		return Arrays.asList(Tuple.from((Object) new byte[] { 0x01, 0x02 }).pack(), Tuple.from("hello").pack(),
		                     Tuple.from("hell\0").pack(), Tuple.from(1066L).pack(), Tuple.from(-1066L).pack(),
		                     Tuple.from(BigInteger.ONE.shiftLeft(Long.SIZE + 1)).pack(),
		                     Tuple.from(BigInteger.ONE.shiftLeft(Long.SIZE + 1).negate()).pack(),
		                     Tuple.from(-3.14f).pack(), Tuple.from(2.71828).pack(),
		                     Tuple.from(new UUID(1066L, 1415L)).pack(),
		                     Tuple
		                         .from(Versionstamp.fromBytes(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		                                                                   0x08, 0x09, 0x0a, 0x0b, 0x0c }))
		                         .pack());
	}

	@Test
	void testEmptyTuple() throws Exception {
		Tuple t = new Tuple();
		Assertions.assertTrue(t.isEmpty(), "Empty tuple is not empty");
		Assertions.assertEquals(0, t.getPackedSize(), "empty tuple packed size is not 0");
		Assertions.assertEquals(0, t.pack().length, "empty tuple is not packed to the empty byte string");
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void packedSizeMatches(Tuple baseTuple, Object newItem) throws Exception {
		Tuple newItemTuple = Tuple.from(newItem);
		Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
		Tuple addedTuple = baseTuple.addObject(newItem);
		Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

		Assertions.assertEquals(baseTuple.getPackedSize() + newItemTuple.getPackedSize(),
		                        mergedWithAddAll.getPackedSize(), "Packed sizes aren't correct for addAll(Tuple)");
		Assertions.assertEquals(baseTuple.getPackedSize() + newItemTuple.getPackedSize(), listTuple.getPackedSize(),
		                        "Packed sizes aren't correct for addAll(Collection)");
		Assertions.assertEquals(baseTuple.getPackedSize() + newItemTuple.getPackedSize(), addedTuple.getPackedSize(),
		                        "Packed sizes aren't correct for addObject()");
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void cannotPackIncorrectlyWithNoIncompleteVersionstamp(Tuple baseTuple, Object newItem) throws Exception {
		Tuple newItemTuple = Tuple.from(newItem);
		Assumptions.assumeTrue(!baseTuple.hasIncompleteVersionstamp(),
		                       "Skipping because baseTuple has an incomplete versionstamp");
		Assumptions.assumeTrue(!newItemTuple.hasIncompleteVersionstamp(),
		                       "Skipping because newItem has an incomplete versionstamp");

		Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
		Tuple addedTuple = baseTuple.addObject(newItem);
		Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

		for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
			try {
				t.packWithVersionstamp();
				Assertions.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
			} catch (IllegalArgumentException expected) {
			}
		}
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void cannotPackIncorrectlyWithAddedItemIncompleteVersionstamp(Tuple baseTuple, Object newItem) throws Exception {
		Tuple newItemTuple = Tuple.from(newItem);
		Assumptions.assumeTrue(!baseTuple.hasIncompleteVersionstamp(),
		                       "Skipping because baseTuple has an incomplete versionstamp");
		Assumptions.assumeTrue(newItemTuple.hasIncompleteVersionstamp(),
		                       "Skipping because newItem has an incomplete versionstamp");

		Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
		Tuple addedTuple = baseTuple.addObject(newItem);
		Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

		for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
			try {
				t.pack();
				Assertions.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
			} catch (IllegalArgumentException expected) {
			}
		}
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void cannotPackIncorrectlyWithBaseTupleIncompleteVersionstamp(Tuple baseTuple, Object newItem) throws Exception {
		Tuple newItemTuple = Tuple.from(newItem);
		Assumptions.assumeTrue(baseTuple.hasIncompleteVersionstamp(),
		                       "Skipping because baseTuple has an incomplete versionstamp");
		Assumptions.assumeTrue(!newItemTuple.hasIncompleteVersionstamp(),
		                       "Skipping because newItem has an incomplete versionstamp");

		Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
		Tuple addedTuple = baseTuple.addObject(newItem);
		Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

		for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
			try {
				t.pack();
				Assertions.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
			} catch (IllegalArgumentException expected) {
			}
		}
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void cannotPackIncorrectlyWithOnlyIncompleteVersionstamp(Tuple baseTuple, Object newItem) throws Exception {
		Tuple newItemTuple = Tuple.from(newItem);
		Assumptions.assumeTrue(baseTuple.hasIncompleteVersionstamp(),
		                       "Skipping because baseTuple has an incomplete versionstamp");
		Assumptions.assumeTrue(newItemTuple.hasIncompleteVersionstamp(),
		                       "Skipping because newItem does not have an incomplete versionstamp");

		Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
		Tuple addedTuple = baseTuple.addObject(newItem);
		Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

		for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
			try {
				t.pack();
				Assertions.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
			} catch (IllegalArgumentException expected) {
			}
		}
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void canAddMethodsFromStream(Tuple baseTuple, Object newItem) throws Exception {
		Tuple freshTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(newItem)));
		Assertions.assertEquals(baseTuple.size() + 1, freshTuple.size(), "Incorrect tuple size after stream concat");
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void canEncodeAddedItemsWithCompleteVersionstamps(Tuple baseTuple, Object toAdd) throws Exception {
		Tuple newTuple = Tuple.from(toAdd);
		// skip this test if we don't fit the appropriate category
		Assumptions.assumeTrue(!baseTuple.hasIncompleteVersionstamp(), "baseTuple has incomplete versionstamp");
		Assumptions.assumeTrue(!newTuple.hasIncompleteVersionstamp(), "addingTuple has incomplete versionstamp");

		byte[] concatPacked = ByteArrayUtil.join(baseTuple.pack(), newTuple.pack());
		byte[] prefixPacked = newTuple.pack(baseTuple.pack());
		byte[] streamPacked = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd))).pack();
		byte[] tupleAddedPacked = baseTuple.addAll(newTuple).pack();
		byte[] listAddedPacked = baseTuple.addAll(Arrays.asList(toAdd)).pack();

		Assertions.assertArrayEquals(concatPacked, prefixPacked, "concatPacked != prefixPacked!");
		Assertions.assertArrayEquals(prefixPacked, streamPacked, "prefixPacked != streamPacked!");
		Assertions.assertArrayEquals(streamPacked, tupleAddedPacked, "streamPacked != tupleAddedPacked!");
		Assertions.assertArrayEquals(tupleAddedPacked, listAddedPacked, "tupleAddedPacked != listAddedPacked!");
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void cannotPackItemsWithCompleteVersionstamps(Tuple baseTuple, Object toAdd) throws Exception {
		Tuple newTuple = Tuple.from(toAdd);
		// skip this test if we don't fit the appropriate category
		Assumptions.assumeTrue(!baseTuple.hasIncompleteVersionstamp(), "baseTuple has incomplete versionstamp");
		Assumptions.assumeTrue(!newTuple.hasIncompleteVersionstamp(), "addingTuple has incomplete versionstamp");

		Tuple streamTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)));
		Tuple aAllTuple = baseTuple.addAll(newTuple);
		Tuple addAllCollTuple = baseTuple.addAll(Arrays.asList(toAdd));
		Tuple addObjectTuple = baseTuple.addObject(toAdd);
		List<Tuple> allTuples = Arrays.asList(streamTuple, aAllTuple, addAllCollTuple, addObjectTuple);

		for (Tuple t : allTuples) {
			Assertions.assertThrows(IllegalArgumentException.class, () -> { t.packWithVersionstamp(); });
		}
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void canEncodeAddedItemsWithIncompleteVersionstamps(Tuple baseTuple, Object toAdd) throws Exception {
		Tuple newTuple = Tuple.from(toAdd);
		// skip this test if we don't fit the appropriate category
		Assumptions.assumeTrue(!baseTuple.hasIncompleteVersionstamp(), "baseTuple has incomplete versionstamp");
		Assumptions.assumeTrue(newTuple.hasIncompleteVersionstamp(), "addingTuple has incomplete versionstamp");

		byte[] prefixPacked = newTuple.packWithVersionstamp(baseTuple.pack());
		byte[] streamPacked =
		    Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd))).packWithVersionstamp();
		byte[] tupleAddedPacked = baseTuple.addAll(newTuple).packWithVersionstamp();
		byte[] listAddedPacked = baseTuple.addAll(Arrays.asList(toAdd)).packWithVersionstamp();

		Assertions.assertArrayEquals(prefixPacked, streamPacked, "prefixPacked != streamPacked!");
		Assertions.assertArrayEquals(streamPacked, tupleAddedPacked, "streamPacked != tupleAddedPacked!");
		Assertions.assertArrayEquals(tupleAddedPacked, listAddedPacked, "tupleAddedPacked != listAddedPacked!");
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void cannotPackItemsWithCompleteVersionstampsForNewItem(Tuple baseTuple, Object toAdd) throws Exception {
		Tuple newTuple = Tuple.from(toAdd);
		// skip this test if we don't fit the appropriate category
		Assumptions.assumeTrue(!baseTuple.hasIncompleteVersionstamp(), "baseTuple has incomplete versionstamp");
		Assumptions.assumeTrue(newTuple.hasIncompleteVersionstamp(), "addingTuple has incomplete versionstamp");

		Tuple streamTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)));
		Tuple aAllTuple = baseTuple.addAll(newTuple);
		Tuple addAllCollTuple = baseTuple.addAll(Arrays.asList(toAdd));
		Tuple addObjectTuple = baseTuple.addObject(toAdd);
		List<Tuple> allTuples = Arrays.asList(streamTuple, aAllTuple, addAllCollTuple, addObjectTuple);

		for (Tuple t : allTuples) {
			try {
				t.pack();
				Assertions.fail("was able to pack tuple without incomplete versionstamps");
			} catch (IllegalArgumentException expected) {
			}
		}
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void canEncodeAddedItemsWithIncompleteTupleVersionstamps(Tuple baseTuple, Object toAdd) throws Exception {
		Tuple newTuple = Tuple.from(toAdd);
		// skip this test if we don't fit the appropriate category
		Assumptions.assumeTrue(baseTuple.hasIncompleteVersionstamp(), "baseTuple has incomplete versionstamp");
		Assumptions.assumeTrue(!newTuple.hasIncompleteVersionstamp(), "addingTuple has incomplete versionstamp");

		byte[] prefixPacked = baseTuple.addObject(toAdd).packWithVersionstamp();
		byte[] streamPacked =
		    Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd))).packWithVersionstamp();
		byte[] tupleAddedPacked = baseTuple.addAll(newTuple).packWithVersionstamp(); // concatPacked
		byte[] listAddedPacked = baseTuple.addAll(Arrays.asList(toAdd)).packWithVersionstamp();

		Assertions.assertArrayEquals(prefixPacked, streamPacked, "prefixPacked != streamPacked!");
		Assertions.assertArrayEquals(streamPacked, tupleAddedPacked, "streamPacked != tupleAddedPacked!");
		Assertions.assertArrayEquals(tupleAddedPacked, listAddedPacked, "tupleAddedPacked != listAddedPacked!");
	}

	@ParameterizedTest
	@MethodSource("baseAddCartesianProduct")
	void cannotPackItemsWithCompleteVersionstampsForBaseTuple(Tuple baseTuple, Object toAdd) throws Exception {
		Tuple newTuple = Tuple.from(toAdd);
		// skip this test if we don't fit the appropriate category
		Assumptions.assumeTrue(baseTuple.hasIncompleteVersionstamp(), "baseTuple has incomplete versionstamp");
		Assumptions.assumeTrue(!newTuple.hasIncompleteVersionstamp(), "addingTuple has incomplete versionstamp");

		Tuple streamTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)));
		Tuple aAllTuple = baseTuple.addAll(newTuple);
		Tuple addAllCollTuple = baseTuple.addAll(Arrays.asList(toAdd));
		Tuple addObjectTuple = baseTuple.addObject(toAdd);
		List<Tuple> allTuples = Arrays.asList(streamTuple, aAllTuple, addAllCollTuple, addObjectTuple);

		for (Tuple t : allTuples) {
			try {
				t.pack();
				Assertions.fail("was able to pack tuple without incomplete versionstamps");
			} catch (IllegalArgumentException expected) {
			}
		}
	}

	@Test
	void testIncompleteVersionstamps() throws Exception {
		Assumptions.assumeTrue(FDB.instance().getAPIVersion() > 520, "Skipping test because version is too old");

		// this is a tricky case where there are two tuples with identical
		// respresentations but different semantics.
		byte[] arr = new byte[0x0100fe];
		Arrays.fill(arr, (byte)0x7f); // the actual value doesn't matter, as long as it's not zero
		Tuple t1 = Tuple.from(arr, Versionstamp.complete(new byte[] { FF, FF, FF, FF, FF, FF, FF, FF, FF, FF }),
		                      new byte[] { 0x01, 0x01 });
		Tuple t2 = Tuple.from(arr, Versionstamp.incomplete());
		Assertions.assertNotEquals(t1, t2, "tuples " + t1 + " and " + t2 + " compared equal");

		byte[] bytes1 = t1.pack();
		byte[] bytes2 = t2.packWithVersionstamp();
		Assertions.assertArrayEquals(bytes1, bytes2,
		                             "tuples " + t1 + " and " + t2 + " did not have matching representations");
		Assertions.assertNotEquals(
		    t1, t2, "tuples " + t1 + " and " + t2 + "+ compared equal with memoized packed representations");
	}

	@Test
	void testPositionInformationAdjustmentForIncompleteVersionstamp() throws Exception {
		// make sure position information adjustment works
		Tuple t3 = Tuple.from(Versionstamp.incomplete(1));
		Assertions.assertEquals(1 + Versionstamp.LENGTH + Integer.BYTES, t3.getPackedSize(),
		                        "incomplete versionstamp has incorrect packed size: " + t3.getPackedSize());

		byte[] bytes3 = t3.packWithVersionstamp();
		Assertions.assertEquals(1,
		                        ByteBuffer.wrap(bytes3, bytes3.length - Integer.BYTES, Integer.BYTES)
		                            .order(ByteOrder.LITTLE_ENDIAN)
		                            .getInt(),
		                        "incomplete versionstamp has incorrect position");
		Assertions.assertEquals(Tuple.from(Versionstamp.incomplete(1)),
		                        Tuple.fromBytes(bytes3, 0, bytes3.length - Integer.BYTES),
		                        "unpacked bytes did not match");

		Subspace subspace = new Subspace(Tuple.from("prefix"));
		byte[] bytes4 = subspace.packWithVersionstamp(t3);
		Assertions.assertEquals(1 + subspace.getKey().length,
		                        ByteBuffer.wrap(bytes4, bytes4.length - Integer.BYTES, Integer.BYTES)
		                            .order(ByteOrder.LITTLE_ENDIAN)
		                            .getInt(),
		                        "incomplete versionstamp has incorrect position with prefix");
		Assertions.assertEquals(Tuple.from("prefix", Versionstamp.incomplete(1)),
		                        Tuple.fromBytes(bytes4, 0, bytes4.length - Integer.BYTES),
		                        "unpacked bytes with subspace did not match");

		try {
			// At this point, the representation is cached, so an easy bug would be to have
			// it return the already serialized value
			t3.pack();
			Assertions.fail("was able to pack versionstamp with incomplete versionstamp");
		} catch (IllegalArgumentException eexpected) {
			// eat
		}
	}

	@ParameterizedTest
	@MethodSource("twoIncomplete")
	void testTwoIncompleteVersionstamps(Tuple t) {
		Assertions.assertTrue(t.hasIncompleteVersionstamp(), "tuple doesn't think is has incomplete versionstamps");
		Assertions.assertTrue(t.getPackedSize() >= 2 * (1 + Versionstamp.LENGTH + Integer.BYTES),
		                      "tuple packed size " + t.getPackedSize() + " is smaller than expected");

		try {
			t.pack();
			Assertions.fail("no error throws when packing any incomplete versionstamps");
		} catch (IllegalArgumentException expected) {
		}

		try {
			t.packWithVersionstamp();
			Assertions.fail("no error thrown when packing with versionstamp with two incompletes");
		} catch (IllegalArgumentException expected) {
		}
	}

	@ParameterizedTest
	@MethodSource("malformedSequences")
	void cantUnpackMalformedSequences(byte[] sequence) {

		try {
			Tuple t = Tuple.fromBytes(sequence);
			Assertions.fail("Able to unpack " + ByteArrayUtil.printable(sequence) + " into " + t);
		} catch (IllegalArgumentException expected) {
		}
	}

	@ParameterizedTest
	@MethodSource("wellFormedSequences")
	void cantUnpackSequencesWithLastCharacter(byte[] sequence) throws Exception {
		Assertions.assertThrows(
		    IllegalArgumentException.class,
		    ()
		        -> Tuple.fromBytes(sequence, 0, sequence.length - 1),
		    String.format("Able to unpack <%s> without last character", ByteArrayUtil.printable(sequence)));
	}

	@Test
	void malformedStrings() throws Exception {
		// Malformed when packing
		List<String> strings = Arrays.asList("\ud83d", // high surrogate without low (end of string)
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
		for (String s : strings) {
			Tuple t = Tuple.from(s);
			try {
				t.getPackedSize();
				Assertions.fail("able to get packed size of malformed string " + ByteArrayUtil.printable(s.getBytes()));
			} catch (IllegalArgumentException expected) {
			}
			try {
				t.pack();
				Assertions.fail("able to pack malformed string " + ByteArrayUtil.printable(s.getBytes()));
			} catch (IllegalArgumentException expected) {
			}
			try {
				// Modify the memoized packed size to match what it would be if naively packed.
				// This checks to make sure the validation logic invoked right before packing
				// works,
				// but getting that code path to execute means modifying the tuple's internal
				// state, hence
				// the reflection.
				Field f = Tuple.class.getDeclaredField("memoizedPackedSize");
				AccessController.doPrivileged((PrivilegedExceptionAction<Void>)() -> {
					if (!f.isAccessible()) {
						f.setAccessible(true);
					}
					f.setInt(t, 2 + s.getBytes("UTF-8").length);
					return null;
				});
				t.pack();
				Assertions.fail("able to pack malformed string");
			} catch (IllegalArgumentException expected) {
				// eat
			}
		}
	}
}
