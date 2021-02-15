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

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * Tests around packing, versionstamps, and assorted encoding-related stuff for tuples.
 */
@RunWith(Theories.class)
public class TupleEncodingTest {
    private static final byte FF = (byte) 0xff;
    @ClassRule
    public static final FDBLibraryRule fdbLib = FDBLibraryRule.v7();

    @DataPoints("baseTuples")
    public static final List<Tuple> baseTuples = Arrays.asList(new Tuple(), Tuple.from(), Tuple.from((Object) null),
            Tuple.from("prefix"), Tuple.from("prefix", null), Tuple.from(new UUID(100, 1000)),
            Tuple.from(Versionstamp.incomplete(1)), Tuple.from(Tuple.from(Versionstamp.incomplete(2))),
            Tuple.from(Collections.singletonList(Versionstamp.incomplete(3))));

    @DataPoints("addItems")
    public static final List<Object> items = Arrays.asList(null, 1066L, BigInteger.valueOf(1066), -3.14f, 2.71828,
            new byte[] { 0x01, 0x02, 0x03 }, new byte[] { 0x01, 0x00, 0x02, 0x00, 0x03 }, "hello there", "hell\0 there",
            "\ud83d\udd25", "\ufb14", false, true, Float.NaN, Float.intBitsToFloat(Integer.MAX_VALUE), Double.NaN,
            Double.longBitsToDouble(Long.MAX_VALUE),
            Versionstamp.complete(new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09 }, 100),
            Versionstamp.incomplete(4), new UUID(-1, 1), Tuple.from((Object) null), Tuple.from("suffix", "tuple"),
            Tuple.from("s\0ffix", "tuple"), Arrays.asList("suffix", "tuple"), Arrays.asList("suffix", null, "tuple"),
            Tuple.from("suffix", null, "tuple"), Tuple.from("suffix", Versionstamp.incomplete(4), "tuple"),
            Arrays.asList("suffix", Arrays.asList("inner", Versionstamp.incomplete(5), "tuple"), "tuple"));

    @DataPoints("twoIncomplete")
    public static List<Tuple> twoIncomplete = Arrays.asList(Tuple.from(Versionstamp.incomplete(1), Versionstamp.incomplete(2)),
                Tuple.from(Tuple.from(Versionstamp.incomplete(3)), Tuple.from(Versionstamp.incomplete(4))),
                new Tuple().add(Versionstamp.incomplete()).add(Versionstamp.incomplete()),
                new Tuple().add(Versionstamp.incomplete()).add(3L).add(Versionstamp.incomplete()),
                Tuple.from(Tuple.from(Versionstamp.incomplete()), "dummy_string")
                        .add(Tuple.from(Versionstamp.incomplete())),
                Tuple.from(Arrays.asList(Versionstamp.incomplete(), "dummy_string"))
                        .add(Tuple.from(Versionstamp.incomplete())),
                Tuple.from(Tuple.from(Versionstamp.incomplete()), "dummy_string")
                        .add(Collections.singletonList(Versionstamp.incomplete())));


    @DataPoints("malformedSequences")
    public static final List<byte[]> malformedSequences = Arrays.asList(
            new byte[] { 0x01, (byte) 0xde, (byte) 0xad, (byte) 0xc0, (byte) 0xde }, // no termination character for
                                                                                     // byte array
            new byte[] { 0x01, (byte) 0xde, (byte) 0xad, 0x00, FF, (byte) 0xc0, (byte) 0xde }, // no termination character but null in middle
            new byte[] { 0x02, 'h', 'e', 'l', 'l', 'o' }, // no termination character for string
            new byte[] { 0x02, 'h', 'e', 'l', 0x00, FF, 'l', 'o' }, // no termination character but null in the middle
            new byte[] { 0x02, 'u', 't', 'f', 0x08, (byte) 0x80, 0x00 }, // invalid utf-8 code point start character
            new byte[] { 0x02, 'u', 't', 'f', 0x08, (byte) 0xc0, 0x01, 0x00 }, // invalid utf-8 code point second character
            // invalid utf-8 (corresponds to high surrogate \ud83d)
            new byte[] { 0x02, 'u', 't', 'f', 0x10, (byte) 0xed, (byte) 0xa0, (byte) 0xbd, (byte) 0x00 }, 
            // invalid utf-8 (corresponds to low surrogate \udd25)
            new byte[] { 0x02, 'u', 't', 'f', 0x10, (byte) 0xed, (byte) 0xb4, (byte) 0xa5, (byte) 0x00 }, 
            // invalid utf-8 (corresponds to \ud83d\udd25 which *is* valid utf-16, but not encoded like that)
            new byte[] { 0x02, 'u', 't', 'f', 0x10, (byte) 0xed, (byte) 0xa0, (byte) 0xbd, (byte) 0xed, (byte) 0xb4, (byte) 0xa5, (byte) 0x00 }, 
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
            new byte[] { FF } // unknown start code
    );

    @DataPoints("well formed sequences")
    public static final List<byte[]> wellFormedSequences = Arrays.asList(
            Tuple.from((Object) new byte[] { 0x01, 0x02 }).pack(), Tuple.from("hello").pack(),
            Tuple.from("hell\0").pack(), Tuple.from(1066L).pack(), Tuple.from(-1066L).pack(),
            Tuple.from(BigInteger.ONE.shiftLeft(Long.SIZE + 1)).pack(),
            Tuple.from(BigInteger.ONE.shiftLeft(Long.SIZE + 1).negate()).pack(), Tuple.from(-3.14f).pack(),
            Tuple.from(2.71828).pack(), Tuple.from(new UUID(1066L, 1415L)).pack(),
            Tuple.from(Versionstamp
                    .fromBytes(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c }))
                    .pack());

    @Test
    public void testEmptyTuple() throws Exception {
        Tuple t = new Tuple();
        Assert.assertTrue("Empty tuple is not empty", t.isEmpty());
        Assert.assertEquals("empty tuple packed size is not 0", 0, t.getPackedSize());
        Assert.assertEquals("empty tuple is not packed to the empty byte string", 0, t.pack().length);
    }

    @Theory
    public void packedSizeMatches(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object newItem) throws Exception {
        Tuple newItemTuple = Tuple.from(newItem);
        Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
        Tuple addedTuple = baseTuple.addObject(newItem);
        Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

        Assert.assertEquals("Packed sizes aren't correct for addAll(Tuple)",
                baseTuple.getPackedSize() + newItemTuple.getPackedSize(), mergedWithAddAll.getPackedSize());
        Assert.assertEquals("Packed sizes aren't correct for addAll(Collection)",
                baseTuple.getPackedSize() + newItemTuple.getPackedSize(), listTuple.getPackedSize());
        Assert.assertEquals("Packed sizes aren't correct for addObject()",
                baseTuple.getPackedSize() + newItemTuple.getPackedSize(), addedTuple.getPackedSize());
    }

    @Theory
    public void cannotPackIncorrectlyWithNoIncompleteVersionstamp(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object newItem) throws Exception {
        Tuple newItemTuple = Tuple.from(newItem);
        Assume.assumeTrue("Skipping because baseTuple has an incomplete versionstamp",
                !baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("Skipping because newItem has an incomplete versionstamp",
                !newItemTuple.hasIncompleteVersionstamp());

        Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
        Tuple addedTuple = baseTuple.addObject(newItem);
        Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

        for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
            try {
                t.packWithVersionstamp();
                Assert.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    @Theory
    public void cannotPackIncorrectlyWithAddedItemIncompleteVersionstamp(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object newItem) throws Exception {
        Tuple newItemTuple = Tuple.from(newItem);
        Assume.assumeTrue("Skipping because baseTuple has an incomplete versionstamp",
                !baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("Skipping because newItem has an incomplete versionstamp",
                newItemTuple.hasIncompleteVersionstamp());

        Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
        Tuple addedTuple = baseTuple.addObject(newItem);
        Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

        for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
            try {
                t.pack();
                Assert.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    @Theory
    public void cannotPackIncorrectlyWithBaseTupleIncompleteVersionstamp(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object newItem) throws Exception {
        Tuple newItemTuple = Tuple.from(newItem);
        Assume.assumeTrue("Skipping because baseTuple has an incomplete versionstamp",
                baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("Skipping because newItem has an incomplete versionstamp",
                !newItemTuple.hasIncompleteVersionstamp());

        Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
        Tuple addedTuple = baseTuple.addObject(newItem);
        Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

        for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
            try {
                t.pack();
                Assert.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    @Theory
    public void cannotPackIncorrectlyWithOnlyIncompleteVersionstamp(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object newItem) throws Exception {
        Tuple newItemTuple = Tuple.from(newItem);
        Assume.assumeTrue("Skipping because baseTuple has an incomplete versionstamp",
                baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("Skipping because newItem has an incomplete versionstamp",
                newItemTuple.hasIncompleteVersionstamp());

        Tuple mergedWithAddAll = baseTuple.addAll(newItemTuple);
        Tuple addedTuple = baseTuple.addObject(newItem);
        Tuple listTuple = baseTuple.addAll(Collections.singletonList(newItem));

        for (Tuple t : Arrays.asList(mergedWithAddAll, addedTuple, listTuple)) {
            try {
                t.pack();
                Assert.fail("able to pack tuple with incomplete versionstamp using packWithVersionstamp");
            } catch (IllegalArgumentException expected) {
            }
        }
    }


    @Theory
    public void canAddMethodsFromStream(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object newItem) throws Exception {
        Tuple freshTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(newItem)));
        Assert.assertEquals("Incorrect tuple size after stream concat", baseTuple.size() + 1, freshTuple.size());
    }

    @Theory
    public void canEncodeAddedItemsWithCompleteVersionstamps(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object toAdd) throws Exception {
        Tuple newTuple = Tuple.from(toAdd);
        // skip this test if we don't fit the appropriate category
        Assume.assumeTrue("baseTuple has incomplete versionstamp", !baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("addingTuple has incomplete versionstamp", !newTuple.hasIncompleteVersionstamp());

        byte[] concatPacked = ByteArrayUtil.join(baseTuple.pack(), newTuple.pack());
        byte[] prefixPacked = newTuple.pack(baseTuple.pack());
        byte[] streamPacked = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd))).pack();
        byte[] tupleAddedPacked = baseTuple.addAll(newTuple).pack();
        byte[] listAddedPacked = baseTuple.addAll(Arrays.asList(toAdd)).pack();

        Assert.assertArrayEquals("concatPacked != prefixPacked!", concatPacked, prefixPacked);
        Assert.assertArrayEquals("prefixPacked != streamPacked!", prefixPacked, streamPacked);
        Assert.assertArrayEquals("streamPacked != tupleAddedPacked!", streamPacked, tupleAddedPacked);
        Assert.assertArrayEquals("tupleAddedPacked != listAddedPacked!", tupleAddedPacked, listAddedPacked);
    }

    @Theory
    public void cannotPackItemsWithCompleteVersionstamps(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object toAdd) throws Exception {
        Tuple newTuple = Tuple.from(toAdd);
        // skip this test if we don't fit the appropriate category
        Assume.assumeTrue("baseTuple has incomplete versionstamp", !baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("addingTuple has incomplete versionstamp", !newTuple.hasIncompleteVersionstamp());

        Tuple streamTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)));
        Tuple aAllTuple = baseTuple.addAll(newTuple);
        Tuple addAllCollTuple = baseTuple.addAll(Arrays.asList(toAdd));
        Tuple addObjectTuple = baseTuple.addObject(toAdd);
        List<Tuple> allTuples = Arrays.asList(streamTuple, aAllTuple, addAllCollTuple, addObjectTuple);

        for (Tuple t : allTuples) {
            try {
                t.packWithVersionstamp();
                Assert.fail("was able to pack tuple without incomplete versionstamps");
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    @Theory
    public void canEncodeAddedItemsWithIncompleteVersionstamps(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object toAdd) throws Exception {
        Tuple newTuple = Tuple.from(toAdd);
        // skip this test if we don't fit the appropriate category
        Assume.assumeTrue("baseTuple has incomplete versionstamp", !baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("addingTuple has incomplete versionstamp", newTuple.hasIncompleteVersionstamp());

        byte[] prefixPacked = newTuple.packWithVersionstamp(baseTuple.pack());
        byte[] streamPacked = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)))
                .packWithVersionstamp();
        byte[] tupleAddedPacked = baseTuple.addAll(newTuple).packWithVersionstamp();
        byte[] listAddedPacked = baseTuple.addAll(Arrays.asList(toAdd)).packWithVersionstamp();

        Assert.assertArrayEquals("prefixPacked != streamPacked!", prefixPacked, streamPacked);
        Assert.assertArrayEquals("streamPacked != tupleAddedPacked!", streamPacked, tupleAddedPacked);
        Assert.assertArrayEquals("tupleAddedPacked != listAddedPacked!", tupleAddedPacked, listAddedPacked);
    }

    @Theory
    public void cannotPackItemsWithCompleteVersionstampsForNewItem(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object toAdd) throws Exception {
        Tuple newTuple = Tuple.from(toAdd);
        // skip this test if we don't fit the appropriate category
        Assume.assumeTrue("baseTuple has incomplete versionstamp", !baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("addingTuple has incomplete versionstamp", newTuple.hasIncompleteVersionstamp());

        Tuple streamTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)));
        Tuple aAllTuple = baseTuple.addAll(newTuple);
        Tuple addAllCollTuple = baseTuple.addAll(Arrays.asList(toAdd));
        Tuple addObjectTuple = baseTuple.addObject(toAdd);
        List<Tuple> allTuples = Arrays.asList(streamTuple, aAllTuple, addAllCollTuple, addObjectTuple);

        for (Tuple t : allTuples) {
            try {
                t.pack();
                Assert.fail("was able to pack tuple without incomplete versionstamps");
            } catch (IllegalArgumentException expected) {
            }
        }
    }

    @Theory
    public void canEncodeAddedItemsWithIncompleteTupleVersionstamps(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object toAdd) throws Exception {
        Tuple newTuple = Tuple.from(toAdd);
        // skip this test if we don't fit the appropriate category
        Assume.assumeTrue("baseTuple has incomplete versionstamp", baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("addingTuple has incomplete versionstamp", !newTuple.hasIncompleteVersionstamp());

        byte[] prefixPacked = baseTuple.addObject(toAdd).packWithVersionstamp();
        byte[] streamPacked = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)))
                .packWithVersionstamp();
        byte[] tupleAddedPacked = baseTuple.addAll(newTuple).packWithVersionstamp(); // concatPacked
        byte[] listAddedPacked = baseTuple.addAll(Arrays.asList(toAdd)).packWithVersionstamp();

        Assert.assertArrayEquals("prefixPacked != streamPacked!", prefixPacked, streamPacked);
        Assert.assertArrayEquals("streamPacked != tupleAddedPacked!", streamPacked, tupleAddedPacked);
        Assert.assertArrayEquals("tupleAddedPacked != listAddedPacked!", tupleAddedPacked, listAddedPacked);
    }

    @Theory
    public void cannotPackItemsWithCompleteVersionstampsForBaseTuple(@FromDataPoints("baseTuples") Tuple baseTuple,
            @FromDataPoints("addItems") Object toAdd) throws Exception {
        Tuple newTuple = Tuple.from(toAdd);
        // skip this test if we don't fit the appropriate category
        Assume.assumeTrue("baseTuple has incomplete versionstamp", baseTuple.hasIncompleteVersionstamp());
        Assume.assumeTrue("addingTuple has incomplete versionstamp", !newTuple.hasIncompleteVersionstamp());

        Tuple streamTuple = Tuple.fromStream(Stream.concat(baseTuple.stream(), Stream.of(toAdd)));
        Tuple aAllTuple = baseTuple.addAll(newTuple);
        Tuple addAllCollTuple = baseTuple.addAll(Arrays.asList(toAdd));
        Tuple addObjectTuple = baseTuple.addObject(toAdd);
        List<Tuple> allTuples = Arrays.asList(streamTuple, aAllTuple, addAllCollTuple, addObjectTuple);

        for (Tuple t : allTuples) {
            try {
                t.pack();
                Assert.fail("was able to pack tuple without incomplete versionstamps");
            } catch (IllegalArgumentException expected) {
            }
        }
    }



    @Test
    public void testIncompleteVersionstamps() throws Exception {
        Assume.assumeTrue("Skipping test because version is too old", FDB.instance().getAPIVersion() > 520);

        // this is a tricky case where there are two tuples with identical
        // respresentations but different semantics.
        byte[] arr = new byte[0x0100fe];
        Arrays.fill(arr, (byte) 0x7f); // the actual value doesn't matter, as long as it's not zero
        Tuple t1 = Tuple.from(arr, Versionstamp.complete(new byte[] { FF, FF, FF, FF, FF, FF, FF, FF, FF, FF }),
                new byte[] { 0x01, 0x01 });
        Tuple t2 = Tuple.from(arr, Versionstamp.incomplete());
        Assert.assertNotEquals("tuples " + t1 + " and " + t2 + " compared equal", t1, t2);

        byte[] bytes1 = t1.pack();
        byte[] bytes2 = t2.packWithVersionstamp();
        Assert.assertArrayEquals("tuples " + t1 + " and " + t2 + " did not have matching representations", bytes1,
                bytes2);
        Assert.assertNotEquals("tuples " + t1 + " and " + t2 + "+ compared equal with memoized packed representations",
                t1, t2);

    }

    @Test
    public void testPositionInformationAdjustmentForIncompleteVersionstamp() throws Exception {
        // make sure position information adjustment works
        Tuple t3 = Tuple.from(Versionstamp.incomplete(1));
        Assert.assertEquals("incomplete versionstamp has incorrect packed size: " + t3.getPackedSize(),
                1 + Versionstamp.LENGTH + Integer.BYTES, t3.getPackedSize());

        byte[] bytes3 = t3.packWithVersionstamp();
        Assert.assertEquals("incomplete versionstamp has incorrect position", 1, ByteBuffer
                .wrap(bytes3, bytes3.length - Integer.BYTES, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt());
        Assert.assertEquals("unpacked bytes did not match", Tuple.from(Versionstamp.incomplete(1)),
                Tuple.fromBytes(bytes3, 0, bytes3.length - Integer.BYTES));

        Subspace subspace = new Subspace(Tuple.from("prefix"));
        byte[] bytes4 = subspace.packWithVersionstamp(t3);
        Assert.assertEquals("incomplete versionstamp has incorrect position with prefix", 1 + subspace.getKey().length,
                ByteBuffer.wrap(bytes4, bytes4.length - Integer.BYTES, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN)
                        .getInt());
        Assert.assertEquals("unpacked bytes with subspace did not match",
                Tuple.from("prefix", Versionstamp.incomplete(1)),
                Tuple.fromBytes(bytes4, 0, bytes4.length - Integer.BYTES));

        try {
            // At this point, the representation is cached, so an easy bug would be to have
            // it return the already serialized value
            t3.pack();
            Assert.fail("was able to pack versionstamp with incomplete versionstamp");
        } catch (IllegalArgumentException eexpected) {
            // eat
        }
    }

    

    @Theory
    public void testTwoIncompleteVersionstamps(@FromDataPoints("twoIncomplete") Tuple t) {
        Assert.assertTrue("tuple doesn't think is has incomplete versionstamps", t.hasIncompleteVersionstamp());
        MatcherAssert.assertThat("tuple packed size " + t.getPackedSize() + " is smaller than expected",
                t.getPackedSize(), Matchers.greaterThanOrEqualTo(2 * (1 + Versionstamp.LENGTH + Integer.BYTES)));

        try {
            t.pack();
            Assert.fail("no error throws when packing any incomplete versionstamps");
        } catch (IllegalArgumentException expected) {
        }

        try {
            t.packWithVersionstamp();
            Assert.fail("no error thrown when packing with versionstamp with two incompletes");
        } catch (IllegalArgumentException expected) {
        }
    }


    @Theory
    public void cantUnpackMalformedSequences(@FromDataPoints("malformedSequences") byte[] sequence) {

        try {
            Tuple t = Tuple.fromBytes(sequence);
            Assert.fail("Able to unpack " + ByteArrayUtil.printable(sequence) + " into " + t);
        } catch (IllegalArgumentException expected) {
        }
    }

    @Theory
    public void cantUnpackSequencesWithLastCharacter(@FromDataPoints("well formed sequences") byte[] sequence)
            throws Exception {
        try {
            Tuple t = Tuple.fromBytes(sequence, 0, sequence.length - 1);
            Assert.fail(String.format("Able to unpack <%s> into <%s> without last characeter",ByteArrayUtil.printable(sequence),t));
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void malformedStrings() throws Exception {
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
                Assert.fail("able to get packed size of malformed string " + ByteArrayUtil.printable(s.getBytes()));
            } catch (IllegalArgumentException expected) {
            }
            try {
                t.pack();
                Assert.fail("able to pack malformed string " + ByteArrayUtil.printable(s.getBytes()));
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
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    if (!f.isAccessible()) {
                        f.setAccessible(true);
                    }
                    f.setInt(t, 2 + s.getBytes("UTF-8").length);
                    return null;
                });
                t.pack();
                Assert.fail("able to pack malformed string");
            } catch (IllegalArgumentException expected) {
                // eat
            }
        }
    }

}
