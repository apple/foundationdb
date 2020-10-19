/*
 * ByteArrayUtil.java
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.apple.foundationdb.Transaction;

/**
 * Utility functions for operating on byte arrays. Although built for
 *  the FoundationDB tuple layer, some functions may be useful otherwise, such as use of
 *  {@link #printable(byte[])} for debugging non-text keys and values.
 *
 */
public class ByteArrayUtil extends FastByteComparisons {
	private static final byte[] EMPTY_BYTES = new byte[0];

	/**
	 * Joins a set of byte arrays into a larger array. The {@code interlude} is placed
	 *  between each of the elements, but not at the beginning or end. In the case that
	 *  the list is empty or {@code null}, a zero-length byte array will be returned.
	 *
	 * @param interlude can be {@code null} or zero length. Placed internally between
	 *  concatenated elements.
	 * @param parts the pieces to be joined. May be {@code null}, but does not allow
	 *  for elements in the list to be {@code null}.
	 * 
	 * @return a newly created concatenation of the input
	 */
	public static byte[] join(byte[] interlude, List<byte[]> parts) {
		return interludeJoin(interlude, parts.toArray(new byte[0][]));
	}
	/**
	 * Joins a set of byte arrays into a larger array. The {@code interlude} is placed
	 *  between each of the elements, but not at the beginning or end. In the case that
	 *  the list is empty or {@code null}, a zero-length byte array will be returned.
	 *
	 * @param interlude can be {@code null} or zero length. Placed internally between
	 *  concatenated elements.
	 * @param parts the pieces to be joined. May be {@code null}, but does not allow
	 *  for elements in the array to be {@code null}.
	 *
	 * @return a newly created concatenation of the input
	 */
	public static byte[] interludeJoin(byte[] interlude, byte[][] parts) {
		if(parts == null)
			return new byte[0];
		int partCount = parts.length;
		if(partCount == 0)
			return EMPTY_BYTES;

		if(interlude == null)
			interlude = EMPTY_BYTES;

		int elementTotals = 0;
		int interludeSize = interlude.length;
		for (int i = 0; i < partCount; i++) {
			elementTotals += parts[i].length;
		}
		byte[] dest = new byte[(interludeSize * (partCount - 1)) + elementTotals];
		int startByte = 0;
		int index = 0;
		for (int i = 0; i < partCount; i++) {
			int length = parts[i].length;
			if(length > 0) {
				System.arraycopy(parts[i], 0, dest, startByte, length);
				startByte += length;
			}
			if(index < partCount - 1 && interludeSize > 0) {
				// If this is not the last element, append the interlude
				System.arraycopy(interlude, 0, dest, startByte, interludeSize);
				startByte += interludeSize;
			}
			index++;
		}
		return dest;
	}

	/**
	 * Joins a variable number of byte arrays into one larger array.
	 *
	 * @param parts the elements to join. {@code null} elements are not allowed.
	 *
	 * @return a newly created concatenation of the input
	 */
	public static byte[] join(byte[]... parts) {
		return interludeJoin(null, parts);
	}

	/**
	 * Tests for the presence of a specific sequence of bytes in a larger array at a
	 *  specific location.<br/>
	 *  If {@code src} is {@code null} there is a case for a match. First, if {@code start}
	 *  is non-zero, an {@code IllegalArgumentException} will be thrown. If {@code start}
	 *  is {@code 0}, will evaluate to {@code true} if {@code pattern} is {@code null};
	 *  {@code false} otherwise.<br/>
	 *  In all other cases, a {@code null} pattern will never match.
	 *
	 * @param src the sequence of bytes in which to search for {@code pattern}
	 * @param start the index at which to look for a match. The length of {@code pattern} added
	 *  to this index must not pass the end of {@code src.}
	 * @param pattern the series of {@code byte}s to match. If {@code null}, will only match
	 *  a {@code null} {@code src} at position {@code 0}.
	 *
	 * @return {@code true} if {@code pattern} is found in {@code src} at {@code start}.
	 */
	static boolean regionEquals(byte[] src, int start, byte[] pattern) {
		if(src == null) {
			if(start == 0) {
				return pattern == null;
			}
			throw new IllegalArgumentException("start index after end of src");
		}
		if(pattern == null)
			return false;

		// At this point neither src or pattern are null...

		if(start >= src.length)
			throw new IllegalArgumentException("start index after end of src");

		if(src.length < start + pattern.length)
			return false;

		return compareTo(src, start, pattern.length, pattern, 0, pattern.length) == 0;
	}

	/**
	 * Replaces occurrences of a pattern in a byte array. Does not mutate the contents
	 *  of the parameter {@code src}.
	 *
	 * @param src the source to search for {@code pattern}
	 * @param pattern the pattern for which to search
	 * @param replacement the sequence of bytes to replace {@code pattern} with.
	 *
	 * @return a newly created array where {@code pattern} replaced with {@code replacement}
	 */
	public static byte[] replace(byte[] src, byte[] pattern, byte[] replacement) {
		if(src == null) {
			return null;
		}
		return replace(src, 0, src.length, pattern, replacement);
	}

	/**
	 * Replaces occurrences of a pattern in a byte array. Does not mutate the contents
	 *  of the parameter {@code src}.
	 *
	 * @param src the source to search for {@code pattern}
	 * @param offset the location in {@code src} at which to start the operation
	 * @param length the number of bytes past {@code offset} to search for {@code pattern}
	 * @param pattern the pattern for which to search
	 * @param replacement the sequence of bytes to replace {@code pattern} with.
	 *
	 * @return a newly created array where {@code pattern} replaced with {@code replacement}
	 */
	public static byte[] replace(byte[] src, int offset, int length,
			byte[] pattern, byte[] replacement) {
		if(offset < 0 || offset > src.length) {
			throw new IllegalArgumentException("Invalid offset for array pattern replacement");
		}
		if(length < 0 || offset + length > src.length) {
			throw new IllegalArgumentException("Invalid length for array pattern replacement");
		}
		if(pattern == null || pattern.length == 0) {
			return Arrays.copyOfRange(src, offset, offset + length);
		}
		ByteBuffer dest;
		if(replacement == null || replacement.length != pattern.length) {
			// Array might change size. This is the "tricky" case.
			int newLength = replace(src, offset, length, pattern, replacement, null);
			if(newLength != length) {
				dest = ByteBuffer.allocate(newLength);
			}
			else {
				// If the array size didn't change, as the pattern and replacement lengths
				// differ, it must be the case that there weren't any occurrences of pattern in src
				// between offset and offset + length, so we can just return a copy.
				return Arrays.copyOfRange(src, offset, offset + length);
			}
		}
		else {
			// No matter what, the array will stay the same size as replacement.length = pattern.length
			dest = ByteBuffer.allocate(length);
		}
		replace(src, offset, length, pattern, replacement, dest);
		return dest.array();
	}

	// Replace any occurrences of pattern in src between offset and offset + length with replacement.
	// The new array is serialized into dest and the new length is returned.
	static int replace(byte[] src, int offset, int length, byte[] pattern, byte[] replacement, ByteBuffer dest) {
		if(pattern == null || pattern.length == 0) {
			if(dest != null) {
				dest.put(src, offset, length);
			}
			return length;
		}
		byte patternFirst = pattern[0];
		int lastPosition = offset;
		int currentPosition = offset;
		int newLength = 0;
		int replacementLength = replacement == null ? 0 : replacement.length;

		while(currentPosition < offset + length) {
			if(src[currentPosition] == patternFirst && regionEquals(src, currentPosition, pattern)) {
				if(dest != null) {
					dest.put(src, lastPosition, currentPosition - lastPosition);
					if(replacement != null) {
						dest.put(replacement);
					}
				}
				newLength += currentPosition - lastPosition + replacementLength;
				currentPosition += pattern.length;
				lastPosition = currentPosition;
			}
			else {
				currentPosition++;
			}
		}

		newLength += currentPosition - lastPosition;
		if(dest != null) {
			dest.put(src, lastPosition, currentPosition - lastPosition);
		}

		return newLength;
	}

	/**
	 * Splits a byte array at each occurrence of a pattern. If the pattern is found at
	 *  the beginning or end of the array the result will have a leading or trailing
	 *  zero-length array. The delimiter is not included in the output array. Does not
	 *  mutate the contents the source array.
	 *
	 * @param src the array to split
	 * @param delimiter the byte pattern on which to split
	 *
	 * @return a list of byte arrays from {@code src} now not containing {@code delimiter}
	 */
	public static List<byte[]> split(byte[] src, byte[] delimiter) {
		return split(src, 0, src.length, delimiter);
	}

	/**
	 * Splits a byte array at each occurrence of a pattern. If the pattern is found at
	 *  the beginning or end of the array the result will have a leading or trailing
	 *  zero-length array. The delimiter is not included in the output array. Does not
	 *  mutate the contents the source array.
	 *
	 * @param src the array to split
	 * @param offset the location in the array at which to start the operation
	 * @param length the number of bytes to search, must not extend past the end of {@code src}
	 * @param delimiter the byte pattern on which to split
	 *
	 * @return a list of byte arrays from {@code src} now not containing {@code delimiter}
	 */
	public static List<byte[]> split(byte[] src, int offset, int length, byte[] delimiter) {
		List<byte[]> parts = new LinkedList<>();
		int idx = offset;
		int lastSplitEnd = offset;
		while(idx <= (offset+length) - delimiter.length) {
			if(regionEquals(src, idx, delimiter)) {
				// copy the last region of bytes into "parts", copyOfRange is happy with zero-sized ranges
				parts.add(Arrays.copyOfRange(src, lastSplitEnd, idx));
				idx += delimiter.length;
				lastSplitEnd = idx;
			} else {
				idx++;
			}
		}
		if(lastSplitEnd == offset + length)
			// if the last replacement ended at the end of src, we need a tailing empty entry
			parts.add(new byte[0]);
		else {
			parts.add(Arrays.copyOfRange(src, lastSplitEnd, offset + length));
		}
		return parts;
	}

	/**
	 * Compare byte arrays for equality and ordering purposes. Elements in the array
	 *  are interpreted and compared as unsigned bytes. Neither parameter
	 *  may be {@code null}.
	 *
	 * @param l byte array on the left-hand side of the inequality
	 * @param r byte array on the right-hand side of the inequality
	 *
	 * @return return -1, 0, or 1 if {@code l} is less than, equal to, or greater than
	 *  {@code r}.
	 */
	public static int compareUnsigned(byte[] l, byte[] r) {
		return compareTo(l, 0, l.length, r, 0, r.length);
	}

	/**
	 * Check if a byte array starts with another byte array.
	 *
	 * @param array the source byte array
	 *
	 * @param prefix the byte array that we are checking if {@code src}
	 *  starts with.
	 *
	 * @return {@code true} if {@code array} starts with {@code prefix}
	 */
	public static boolean startsWith(byte[] array, byte[] prefix) {
		// Short Circuit
		if(array.length < prefix.length) {
			return false;
		}
		return compareTo(array, 0, prefix.length, prefix, 0, prefix.length) == 0;
	}

	/**
	 * Computes the first key that would sort outside the range prefixed by {@code key}.
	 *  {@code key} must be non-null, and contain at least some character this is not
	 *  {@code \xFF} (255).
	 *
	 * @param key prefix key
	 *
	 * @return a newly created byte array
	 */
	public static byte[] strinc(byte[] key) {
		byte[] copy = rstrip(key, (byte)0xff);
		if(copy.length == 0)
			throw new IllegalArgumentException("No key beyond supplied prefix");

		// Since rstrip makes sure the last character is not \xff, we can be sure
		//  we're able to add 1 to it without overflow.
		copy[copy.length -1] = (byte) (copy[copy.length - 1] + 1);
		return copy;
	}

	/**
	 * Computes the key that would sort immediately after {@code key}.
	 *  {@code key} must be non-null.
	 *
	 * @param key byte array for which next key is to be computed
	 *
	 * @return a newly created byte array that would sort immediately after {@code key}
	*/
	public static byte[] keyAfter(byte[] key) {
		byte[] copy = new byte[key.length + 1];
		System.arraycopy(key, 0, copy, 0, key.length);
		copy[key.length] = 0x0;
		return copy;
	}

	/**
	 * Get a copy of an array, with all matching characters stripped from trailing edge.
	 * @param input array to copy. Must not be null.
	 * @param target byte to exclude from copy.
	 * @return returns a copy of {@code input} excluding occurrences of {@code target}
	 *  at the end.
	 */
	static byte[] rstrip(byte[] input, byte target) {
		int i = input.length - 1;
		for(; i >= 0; i--) {
			if(input[i] != target)
				break;
		}
		return Arrays.copyOfRange(input, 0, i + 1);
	}

	/**
	 * Encode an 64-bit integer (long) into a byte array. Encodes the integer in little
	 *  endian byte order. The result is valid for use with
	 *  {@link Transaction#mutate(com.apple.foundationdb.MutationType, byte[], byte[]) Transaction.mutate(...)}.
	 *
	 * @param i the number to encode
	 * @return an 8-byte array containing the
	 *
	 * @see Transaction#mutate(com.apple.foundationdb.MutationType, byte[], byte[])
	 */
	public static byte[] encodeInt(long i) {
		return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(i).array();
	}

	/**
	 * Decode a little-endian encoded long integer from an 8-byte array.
	 *
	 * @param src the non-null, 8-element byte array from which to decode
	 * @return a decoded 64-bit integer
	 */
	public static long decodeInt(byte[] src) {
		if(src.length != 8) {
			throw new IllegalArgumentException("Source array must be of length 8");
		}
		return ByteBuffer.wrap(src).order(ByteOrder.LITTLE_ENDIAN).getLong();
	}

	/**
	 * Gets a human readable version of a byte array. The bytes that correspond with
	 *  ASCII printable characters [32-127) are passed through. Other bytes are
	 *  replaced with {@code \x} followed by a two character zero-padded hex code for the
	 *  byte.
	 *
	 * @param val the byte array for which to create a human readable form
	 *
	 * @return a modification of the byte array with unprintable characters replaced.
	 */
	public static String printable(byte[] val) {
		if(val == null)
			return null;
		StringBuilder s = new StringBuilder();
		for(int i=0; i<val.length; i++) {
			byte b = val[i];
			if (b >= 32 && b < 127 && b != '\\') s.append((char)b);
			else if (b == '\\') s.append("\\\\");
			else s.append(String.format("\\x%02x", b));
		}
		return s.toString();
	}

	static int nullCount(byte[] val) {
		int nulls = 0;
		for(int i = 0; i < val.length; i++) {
			if(val[i] == 0x00)
				nulls += 1;
		}
		return nulls;
	}

	private ByteArrayUtil() {}
}
