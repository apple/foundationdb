/*
 * FastByteComparisons.java
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
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;

import sun.misc.Unsafe;


/**
 * Utility code to do optimized byte-array comparison.
 * This is borrowed and slightly modified from Guava's UnsignedBytes
 * class to be able to compare arrays that start at non-zero offsets.
 */
abstract class FastByteComparisons {

    private static final int UNSIGNED_MASK = 0xFF;
    /**
     * Lexicographically compare two byte arrays.
     *
     * @param buffer1 left operand, expected to not be null
     * @param buffer2 right operand, expected to not be null
     * @param offset1 Where to start comparing in the left buffer, expected to be &gt;= 0
     * @param offset2 Where to start comparing in the right buffer, expected to be &gt;= 0
     * @param length1 How much to compare from the left buffer, expected to be &gt;= 0
     * @param length2 How much to compare from the right buffer, expected to be &gt;= 0
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static int compareTo(byte[] buffer1, int offset1, int length1,
            byte[] buffer2, int offset2, int length2) {
        return LexicographicalComparerHolder.BEST_COMPARER.compareTo(
                buffer1, offset1, length1, buffer2, offset2, length2);
    }
    /**
     * Interface for both the java and unsafe comparators + offset based comparisons.
     * @param <T>
     */
    interface Comparer<T> extends Comparator<T> {
        /**
         * Lexicographically compare two byte arrays.
         *
         * @param buffer1 left operand
         * @param buffer2 right operand
         * @param offset1 Where to start comparing in the left buffer
         * @param offset2 Where to start comparing in the right buffer
         * @param length1 How much to compare from the left buffer
         * @param length2 How much to compare from the right buffer
         * @return 0 if equal, < 0 if left is less than right, etc.
         */
        abstract public int compareTo(T buffer1, int offset1, int length1,
                                      T buffer2, int offset2, int length2);
    }

    /**
     * Pure Java Comparer
     *
     * @return
     */
    static Comparer<byte[]> lexicographicalComparerJavaImpl() {
        return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
    }

    /**
     * Unsafe Comparer
     *
     * @return
     */
    static Comparer<byte[]> lexicographicalComparerUnsafeImpl() {
        return LexicographicalComparerHolder.UnsafeComparer.INSTANCE;
    }


    /**
     * Provides a lexicographical comparer implementation; either a Java
     * implementation or a faster implementation based on {@link Unsafe}.
     *
     * <p>Uses reflection to gracefully fall back to the Java implementation if
     * {@code Unsafe} isn't available.
     */
    private static class LexicographicalComparerHolder {
        static final String UNSAFE_COMPARER_NAME =
                LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";

        static final Comparer<byte[]> BEST_COMPARER = getBestComparer();
        /**
         * Returns the Unsafe-using Comparer, or falls back to the pure-Java
         * implementation if unable to do so.
         */
        static Comparer<byte[]> getBestComparer() {
            String arch = System.getProperty("os.arch");
            boolean unaligned = arch.equals("i386") || arch.equals("x86")
                    || arch.equals("amd64") || arch.equals("x86_64");
            if (!unaligned)
                return lexicographicalComparerJavaImpl();
            try {
                Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

                // yes, UnsafeComparer does implement Comparer<byte[]>
                @SuppressWarnings("unchecked")
                Comparer<byte[]> comparer =
                        (Comparer<byte[]>) theClass.getEnumConstants()[0];
                return comparer;
            } catch (Throwable t) { // ensure we really catch *everything*
                return lexicographicalComparerJavaImpl();
            }
        }

        /**
         * Java Comparer doing byte by byte comparisons
         *
         */
        enum PureJavaComparer implements Comparer<byte[]> {
            INSTANCE;

            /**
             *
             * CompareTo looking at two buffers.
             *
             * @param buffer1 left operand
             * @param buffer2 right operand
             * @param offset1 Where to start comparing in the left buffer
             * @param offset2 Where to start comparing in the right buffer
             * @param length1 How much to compare from the left buffer
             * @param length2 How much to compare from the right buffer
             * @return 0 if equal, < 0 if left is less than right, etc.
             */
            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1,
                                 byte[] buffer2, int offset2, int length2) {
                // Short circuit equal case
                if (buffer1 == buffer2 &&
                        offset1 == offset2 &&
                        length1 == length2) {
                    return 0;
                }
                int end1 = offset1 + length1;
                int end2 = offset2 + length2;
                for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                    int a = (buffer1[i] & UNSIGNED_MASK);
                    int b = (buffer2[j] & UNSIGNED_MASK);
                    if (a != b) {
                        return a - b;
                    }
                }
                return length1 - length2;
            }

            /**
             * Supports Comparator
             *
             * @param o1
             * @param o2
             * @return comparison
             */
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return compareTo(o1, 0, o1.length, o2, 0, o2.length);
            }
        }

        /**
         *
         * Takes advantage of word based comparisons
         *
         */
        @SuppressWarnings("unused") // used via reflection
        enum UnsafeComparer implements Comparer<byte[]> {
            INSTANCE;

            static final Unsafe theUnsafe;

            /**
             * The offset to the first element in a byte array.
             */
            static final int BYTE_ARRAY_BASE_OFFSET;

            @Override
            public int compare(byte[] o1, byte[] o2) {
                return compareTo(o1, 0, o1.length, o2, 0, o2.length);
            }

            static {
                theUnsafe = (Unsafe) AccessController.doPrivileged(
                        (PrivilegedAction<Object>) () -> {
                            try {
                                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                                f.setAccessible(true);
                                return f.get(null);
                            } catch (NoSuchFieldException e) {
                                // It doesn't matter what we throw;
                                // it's swallowed in getBestComparer().
                                throw new Error();
                            } catch (IllegalAccessException e) {
                                throw new Error();
                            }
                        });

                BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

                // sanity check - this should never fail
                if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
                    throw new AssertionError();
                }
            }

            static final boolean LITTLE_ENDIAN =
                    ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

            /**
             * Lexicographically compare two arrays.
             *
             * @param buffer1 left operand
             * @param buffer2 right operand
             * @param offset1 Where to start comparing in the left buffer
             * @param offset2 Where to start comparing in the right buffer
             * @param length1 How much to compare from the left buffer
             * @param length2 How much to compare from the right buffer
             * @return 0 if equal, < 0 if left is less than right, etc.
             */
            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1,
                                 byte[] buffer2, int offset2, int length2) {
                // Short circuit equal case
                if (buffer1 == buffer2 &&
                        offset1 == offset2 &&
                        length1 == length2) {
                    return 0;
                }
                final int stride = 8;
                final int minLength = Math.min(length1, length2);
                int strideLimit = minLength & ~(stride - 1);
                final long offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
                final long offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;
                int i;

                /*
                 * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
                 * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
                 */
                for (i = 0; i < strideLimit; i += stride) {
                    long lw = theUnsafe.getLong(buffer1, offset1Adj + i);
                    long rw = theUnsafe.getLong(buffer2, offset2Adj + i);
                    if (lw != rw) {
                        if(!LITTLE_ENDIAN) {
                            return ((lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE)) ? -1 : 1;
                        }

                        /*
                         * We want to compare only the first index where left[index] != right[index]. This
                         * corresponds to the least significant nonzero byte in lw ^ rw, since lw and rw are
                         * little-endian. Long.numberOfTrailingZeros(diff) tells us the least significant
                         * nonzero bit, and zeroing out the first three bits of L.nTZ gives us the shift to get
                         * that least significant nonzero byte. This comparison logic is based on UnsignedBytes
                         * comparator from guava v21
                         */
                        int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
                        return ((int) ((lw >>> n) & UNSIGNED_MASK)) - ((int) ((rw >>> n) & UNSIGNED_MASK));
                    }
                }

                // The epilogue to cover the last (minLength % stride) elements.
                for (; i < minLength; i++) {
                    int a = (buffer1[offset1 + i] & UNSIGNED_MASK);
                    int b = (buffer2[offset2 + i] & UNSIGNED_MASK);
                    if (a != b) {
                        return a - b;
                    }
                }
                return length1 - length2;
            }
        }
    }
}
