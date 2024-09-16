/*
 * Subspace.java
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

package com.apple.foundationdb.subspace;

import static com.apple.foundationdb.tuple.ByteArrayUtil.join;
import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;

import java.util.Arrays;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

/**
 * {@code Subspace} provide a convenient way to use {@link Tuple}s to define namespaces for
 * different categories of data. The namespace is specified by a prefix {@link Tuple}
 * which is prepended to all {@link Tuple}s packed by the {@code Subspace}. When unpacking a key
 * with the {@code Subspace}, the prefix {@link Tuple} will be removed from the result.
 *
 * <p>
 *   For general guidance on subspace usage, see the discussion in
 *   <a href="/foundationdb/developer-guide.html#developer-guide-sub-keyspaces" target="_blank">Developer Guide</a>.
 * </p>
 *
 * <p>
 *   As a best practice, API clients should use at least one subspace for application data.
 * </p>
 */
public class Subspace {
	private static final Tuple EMPTY_TUPLE = Tuple.from();
	private static final byte[] EMPTY_BYTES = new byte[0];

	private final byte[] rawPrefix;

	/**
	 * Constructor for a subspace formed with an empty prefix {@link Tuple}.
	 */
	public Subspace() {
		this(EMPTY_TUPLE, EMPTY_BYTES);
	}

	/**
	 * Constructor for a subspace formed with the specified prefix {@link Tuple}.
	 * Note that the {@link Tuple} {@code prefix} should not contain any incomplete
	 * {@link Versionstamp}s as any of its entries.
	 *
	 * @param prefix a {@link Tuple} used to form the subspace
	 * @throws IllegalArgumentException if {@code prefix} contains any incomplete {@link Versionstamp}s
	 */
	public Subspace(Tuple prefix) {
		this(prefix, EMPTY_BYTES);
	}

	/**
	 * Constructor for a subspace formed with the specified byte string, which will
	 * be prepended to all packed keys.
	 *
	 * @param rawPrefix a byte array used as the prefix for all packed keys
	 */
	public Subspace(byte[] rawPrefix) {
		this(EMPTY_TUPLE, rawPrefix);
	}

	/**
	 * Constructor for a subspace formed with both a prefix {@link Tuple} and a
	 * prefix byte string. The prefix {@code Tuple} will be prepended to all
	 * {@code Tuples} packed by the {@code Subspace}, and the byte string prefix
	 * will be prepended to the packed result. Note that the {@link Tuple} {@code prefix}
	 * should not contain any incomplete {@link Versionstamp}s as any of its entries.
	 *
	 * @param prefix a {@code Tuple} used to form the subspace
	 * @param rawPrefix a byte array used as the prefix for all packed keys
	 * @throws IllegalArgumentException if {@code prefix} contains any incomplete {@link Versionstamp}s
	 */
	public Subspace(Tuple prefix, byte[] rawPrefix) {
		this.rawPrefix = join(rawPrefix, prefix.pack());
	}

	/**
	 * Returns true if this {@code Subspace} is equal to {@code rhs}.
	 * Two {@code Subspace}s are equal if they have the same prefix.
	 *
	 * @param rhs the object to check for equality
	 * @return {@code true} if this {@code Subspace} and {@code rhs} have equal prefixes
	 */
	@Override
	public boolean equals(Object rhs) {
		if(this == rhs) {
			return true;
		}
		if(rhs == null || getClass() != rhs.getClass()) {
			return false;
		}
		Subspace other = (Subspace)rhs;
		return Arrays.equals(rawPrefix, other.rawPrefix);
	}

	/**
	 * Create a human-readable string representation of this subspace. This is
	 * really only useful for debugging purposes, but it includes information
	 * on what raw prefix the subspace is using.
	 * @return a printable representation of the subspace
	 */
	@Override
	public String toString() {
		return "Subspace(rawPrefix=" + printable(rawPrefix) + ")";
	}

	/**
	 * Returns a hash-table compatible hash of this subspace. This is based off
	 * of the hash of the underlying byte-array prefix.
	 * @return a hash of this subspace
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(rawPrefix);
	}

	/**
	 * Gets a new subspace which is equivalent to this subspace with its prefix {@link Tuple} extended by
	 * the specified {@code Object}. The object will be inserted into a {@link Tuple} and passed to {@link #get(Tuple)}.
	 *
	 * @param obj an {@code Object} compatible with {@code Tuple}s
	 * @return a new subspace formed by joining this {@code Subspace}'s prefix to {@code obj}
	 */
	public Subspace get(Object obj) {
		return get(Tuple.from(obj));
	}

	/**
	 * Gets a new subspace which is equivalent to this subspace with its prefix {@link Tuple} extended by
	 * the specified {@link Tuple}.
	 *
	 * @param tuple the {@link Tuple} used to form the new {@code Subspace}
	 * @return a new subspace formed by joining this {@code Subspace}'s prefix to {@code tuple}
	 */
	public Subspace get(Tuple tuple) {
		return subspace(tuple);
	}

	/**
	 * Gets the key encoding the prefix used for this {@code Subspace}. This is equivalent to
	 * {@link #pack}ing the empty {@link Tuple}.
	 *
	 * @return the key encoding the prefix used for this {@code Subspace}
	 */
	public byte[] getKey() {
		return pack();
	}

	/**
	 * Gets the key encoding the prefix used for this {@code Subspace}.
	 *
	 * @return the key encoding the prefix used for this {@code Subspace}
	 */
	public byte[] pack() {
		return Arrays.copyOf(rawPrefix, rawPrefix.length);
	}

	/**
	 * Gets the key encoding the specified {@code Object} in this {@code Subspace}. {@code obj} is
	 * inserted into a {@link Tuple} and packed with {@link #pack(Tuple)}.
	 *
	 * @param obj an {@code Object} to be packed that is compatible with {@link Tuple}s
	 * @return the key encoding the tuple derived from {@code obj}
	 */
	public byte[] pack(Object obj) {
		return pack(Tuple.from(obj));
	}

	/**
	 * Gets the key encoding the specified tuple in this {@code Subspace}. For example, if you have a {@code Subspace}
	 * with prefix {@link Tuple} {@code ("users")} and you use it to pack the {@link Tuple} {@code ("Smith")},
	 * the result is the same as if you packed the {@link Tuple} {@code ("users", "Smith")}.
	 *
	 * @param tuple the {@code Tuple} to be packed
	 * @return the key encoding the specified tuple in this {@code Subspace}
	 */
	public byte[] pack(Tuple tuple) {
		return tuple.pack(rawPrefix);
	}

	/**
	 * Gets the key encoding the specified tuple in this {@code Subspace} for use with
	 * {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY MutationType.SET_VERSIONSTAMPED_KEY}.
	 * There must be exactly one incomplete {@link Versionstamp} included in the given {@link Tuple}. It will
	 * create a key that is within this {@code Subspace} that can be provided as the {@code key} argument to
	 * {@link com.apple.foundationdb.Transaction#mutate(com.apple.foundationdb.MutationType, byte[], byte[]) Transaction.mutate()}
	 * with the {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
	 * mutation. This will throw an {@link IllegalArgumentException} if the {@link Tuple} does not
	 * contain an incomplete {@link Versionstamp} or if it contains multiple.
	 *
	 * @param tuple the {@code Tuple} to be packed
	 * @return the key encoding the specified tuple in this {@code Subspace}
	 * @throws IllegalArgumentException if {@code tuple} does not contain exactly one incomplete {@link Versionstamp}
	 */
	public byte[] packWithVersionstamp(Tuple tuple) {
		return tuple.packWithVersionstamp(rawPrefix);
	}

	/**
	 * Gets the {@link Tuple} encoded by the given key, with this {@code Subspace}'s prefix {@link Tuple} and
	 * {@code raw prefix} removed.
	 *
	 * @param key The key being decoded
	 * @return the {@link Tuple} encoded by {@code key} with the prefix removed
	 */
	public Tuple unpack(byte[] key) {
		if(!contains(key))
			throw new IllegalArgumentException("Cannot unpack key that is not contained in subspace.");

		return Tuple.fromBytes(key, rawPrefix.length, key.length - rawPrefix.length);
	}

	/**
	 * Gets a {@link Range} representing all keys strictly in the {@code Subspace}.
	 *
	 * @return the {@link Range} of keyspace corresponding to this {@code Subspace}
	 */
	public Range range() {
		return range(EMPTY_TUPLE);
	}

	/**
	 * Gets a {@link Range} representing all keys in the {@code Subspace} strictly starting with
	 * the specified {@link Tuple}.
	 *
	 * @param tuple the {@code Tuple} whose sub-keys we are searching for
	 * @return the {@link Range} of keyspace corresponding to {@code tuple}
	 */
	public Range range(Tuple tuple) {
		return tuple.range(rawPrefix);
	}

	/**
	 * Tests whether the specified key starts with this {@code Subspace}'s prefix, indicating that
	 * the {@code Subspace} logically contains key.
	 *
	 * @param key the key to be tested
	 * @return {@code true} if {@code key} starts with {@code Subspace.key()}
	 */
	public boolean contains(byte[] key) {
		return ByteArrayUtil.startsWith(key, rawPrefix);
	}

	/**
	 * Gets a new subspace which is equivalent to this subspace with its prefix {@link Tuple} extended by
	 * the specified {@link Tuple}.
	 *
	 * @param tuple the {@link Tuple} used to form the new {@code Subspace}
	 * @return a new subspace formed by joining this {@code Subspace}'s prefix to {@code tuple}
	 */
	public Subspace subspace(Tuple tuple) {
		return new Subspace(tuple, rawPrefix);
	}
}
