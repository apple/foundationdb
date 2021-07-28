/*
 * KeySelector.java
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

import com.apple.foundationdb.tuple.ByteArrayUtil;

/**
 * A {@code KeySelector} identifies a particular key in the database. FoundationDB's
 *  lexicographically ordered data model permits finding keys based on their order (for
 *  example, finding the first key in the database greater than a given key). Key selectors
 *  represent a description of a key in the database that could be resolved to an actual
 *  key by {@code Transaction}'s {@link Transaction#getKey(KeySelector) getKey()}
 *  or used directly as the beginning or end of a range in {@code Transaction}'s
 *  {@link Transaction#getRange(KeySelector, KeySelector) getRange()}.<br>
 * <br>
 * For more about how key selectors work in practice, see
 * <a href="/foundationdb/developer-guide.html#key-selectors" target="_blank">the KeySelector documentation</a>.
 *  Note that the way the key selectors are resolved is somewhat non-intuitive, so
 *  users who wish to use a key selector other than the default ones described below should
 *  probably consult that documentation before proceeding.
 * <br>
 * <br>
 * Generally one of the following static methods should be used to construct a {@code KeySelector}:
 *  <ul><li>{@link #lastLessThan(byte[]) lastLessThan}</li>
 *  <li>{@link #lastLessOrEqual(byte[]) lastLessOrEqual}</li>
 *  <li>{@link #firstGreaterThan(byte[]) firstGreaterThan}</li>
 *  <li>{@link #firstGreaterOrEqual(byte[]) firstGreaterOrEqual}</li></ul>
 *  <br>
 *  This is an <i>immutable</i> class.  The {@code add(int)} call does not
 *   modify internal state, but returns a new instance.
 *  <br>
 */
public class KeySelector {
	private final byte[] key;
	private final boolean orEqual;
	private final int offset;

	/**
	 * Constructs a new {@code KeySelector} from the given parameters.  Client code
	 *  will not generally call this constructor. A key selector can be used to
	 *  specify a key that will be resolved at runtime based on a starting key and
	 *  an offset. When this is passed as an argument to a {@link Transaction}'s
	 *  {@link Transaction#getKey(KeySelector) getKey()} or
	 *  {@link Transaction#getRange(KeySelector, KeySelector) getRange()}
	 *  methods, the key selector will be resolved to a key within the
	 *  database. This is done in a manner equivalent to finding the last key that is
	 *  less than (or less than or equal to, if {@code orEqual} is
	 *  {@code true}) the base {@code key} specified here and then
	 *  returning the key that is {@code offset} keys greater than that
	 *  key.
	 *
	 * @param key the base key to reference
	 * @param orEqual {@code true} if the key selector should resolve to
	 *                {@code key} (if {@code key} is present) before accounting for the offset
	 * @param offset the offset (in number of keys) that the selector will advance after
	 *               resolving to a key based on the {@code key} and {@code orEqual} parameters
	 */
	public KeySelector(byte[] key, boolean orEqual, int offset) {
		this.key = key;
		this.orEqual = orEqual;
		this.offset = offset;
	}

	/**
	 * Creates a {@code KeySelector} that picks the last key less than the parameter
	 *
	 * @param key the key to use as the edge of the edge of selection criteria
	 *
	 * @return a newly created {@code KeySelector}
	 */
	public static KeySelector lastLessThan(byte[] key) {
		return new KeySelector(key, false, 0);
	}

	/**
	 * Creates a {@code KeySelector} that picks the last key less than or equal to the parameter
	 *
	 * @param key the key to use as the edge of the edge of selection criteria
	 *
	 * @return a newly created {@code KeySelector}
	 */
	public static KeySelector lastLessOrEqual(byte[] key) {
		return new KeySelector(key, true, 0);
	}

	/**
	 * Creates a {@code KeySelector} that picks the first key greater than the parameter
	 *
	 * @param key the key to use as the edge of the edge of selection criteria
	 *
	 * @return a newly created {@code KeySelector}
	 */
	public static KeySelector firstGreaterThan(byte[] key) {
		return new KeySelector(key, true, +1);
	}

	/**
	 * Creates a {@code KeySelector} that picks the first key greater than or equal to the parameter
	 *
	 * @param key the key to use as the edge of the edge of selection criteria
	 *
	 * @return a newly created {@code KeySelector}
	 */
	public static KeySelector firstGreaterOrEqual(byte[] key) {
		return new KeySelector(key, false, +1);
	}

	/**
	 * Returns a new {@code KeySelector} offset by a given
	 *  number of keys from this one. For example, an offset of {@code 1} means
	 *  that the new {@code KeySelector} specifies the key in the database
	 *  after the key selected by this {@code KeySelector}. The offset can be negative;
	 *  these will move the selector to previous keys in the database.<br>
	 * <br>
	 * Note that large offsets take time O(offset) to resolve, making them a
	 *  poor choice for iterating through a large range. (Instead, use the keys
	 *  returned from a range query operation
	 *  themselves to create a new beginning {@code KeySelector}.) For more information see
	 *  <a href="/foundationdb/developer-guide.html#key-selectors" target="_blank">the KeySelector documentation</a>.
	 *
	 * @param offset the number of keys to offset the {@code KeySelector}. This number can be
	 *  negative.
	 *
	 * @return a newly created {@code KeySelector} that is offset by a number of keys.
	 */
	public KeySelector add(int offset) {
		return new KeySelector(getKey(), orEqual(), getOffset() + offset);
	}

	/**
	 * Returns a copy of the key that serves as the anchor for this {@code KeySelector}. This is
	 *  <i>not</i> the key to which this {@code KeySelector} would resolve to. For this
	 *  function see {@link ReadTransaction#getKey(KeySelector)}.
	 *
	 * @return a copy of the "anchor" key for this {@code KeySelector}.
	 */
	public byte[] getKey() {
		byte[] res = new byte[key.length];
		System.arraycopy(key, 0, res, 0, key.length);
		return res;
	}

	@Override
	public String toString() {
		return String.format("(%s, %s, %d)",
				ByteArrayUtil.printable(getKey()), Boolean.toString(orEqual()), getOffset());
	}

	/**
	 * Returns the orEqual parameter for this {@code KeySelector}. See the
	 * {@link #KeySelector(byte[], boolean, int)} KeySelector constructor}
	 * for more details.
	 *
	 * @return the {@code or-equal} parameter of this {@code KeySelector}.
	 */
	public boolean orEqual() {
		return orEqual;
	}

	/**
	 * Returns the key offset parameter for this {@code KeySelector}. See
	 *  the {@link #KeySelector(byte[], boolean, int) KeySelector constructor}
	 *  for more details.
	 *
	 * @return the key offset for this {@code KeySelector}
	 */
	public int getOffset() {
		return offset;
	}
}
