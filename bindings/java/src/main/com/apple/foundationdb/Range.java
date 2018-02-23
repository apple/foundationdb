/*
 * Range.java
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

package com.apple.foundationdb;

import java.util.Arrays;

import com.apple.foundationdb.tuple.ByteArrayUtil;

/**
 * A simple description of an exact range of keyspace, specified by a begin and end key. As with
 *  all FoundationDB APIs, {@code begin} is inclusive, {@code end} exclusive.
 *
 */
public class Range {
	/**
	 * The beginning of the range. This constraint on the range is inclusive.
	 */
	public final byte[] begin;

	/**
	 * The end of the range. This constraint on the range is exclusive.
	 */
	public final byte[] end;

	/**
	 * Construct a new {@code Range} with an inclusive begin key and an exclusive
	 *  end key.
	 * @param begin the inclusive beginning of the range.
	 * @param end the exclusive end of the range.
	 */
	public Range(byte[] begin, byte[] end) {
		this.begin = begin;
		this.end = end;
	}

	/**
	 * Returns a {@code Range} that describes all possible keys that are prefixed with a
	 *  specified key. Use the result of this call as an input to
	 *  {@link Transaction#getRange(Range)} to replicate the now-removed call
	 *  {@code Transaction.getRangeStartsWith(k)}.
	 *
	 * @param prefix the key prefixing the range, must not be {@code null}
	 *
	 * @return the range of keys starting with {@code prefix}
	 */
	public static Range startsWith(byte[] prefix) {
		if(prefix == null)
			throw new NullPointerException("prefix cannot be null");
		return new Range(prefix, ByteArrayUtil.strinc(prefix));
	}

	/**
	 * Returns {@code true} if the given {@link Object} is a {@code Range}
	 *  object that refers to the same key range within the keyspace.
	 *  This will be true if the given range has the same {@link #begin}
	 *  and {@link #end} key. This will return {@code false} if the given
	 *  {@link Object} is not a {@code Range} instance.
	 *
	 * @param o the {@link Object} to check for equality
	 *
	 * @return whether the given {@link Object} matches this {@code Range}
	 */
	@Override
	public boolean equals(Object o) {
		if(this == o) {
			return true;
		}
		else if(o == null || !(o instanceof Range)) {
			return false;
		}
		else {
			Range that = (Range)o;
			return Arrays.equals(this.begin, that.begin) && Arrays.equals(this.end, that.end);
		}
	}

	/**
	 * Computes a hash code from the {@link #begin} and {@link #end} keys of
	 *  this {@code Range}. In particular, it will take the bitwise XOR of the
	 *  hash of {@link #begin} with 37 times the hash of {@link #end}.
	 *
	 * @return hash code derived from the hashes of {@link #begin} and {@link #end}
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(begin) ^ (37 * Arrays.hashCode(end));
	}

	/**
	 * Returns a human-readable {@link String} representation of this {@code Range}.
	 *  It will contain human-readable representations of both the {@link #begin}
	 *  and {@link #end} keys.
	 *
	 * @return a human-readable representation of this {@code Range}
	 */
	@Override
	public String toString() {
		return "Range(" + (begin == null ? "null" : "\"" + ByteArrayUtil.printable(begin) + "\"") +
				", " + (end == null ? "null" : "\"" + ByteArrayUtil.printable(end) + "\"") + ")";
	}
}