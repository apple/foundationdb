/*
 * Versionstamp.java
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

/**
 * Used to represent values written by versionstamp operations within a {@link Tuple}.
 *  This wraps a single array which should contain 12 bytes. The first 10 bytes
 *  are used by the transaction resolver to impose a globally ordered version. The first
 *  eight of those bytes are used to specify the transaction version, and the next two
 *  are used to specify the version of the transaction within a commit batch.
 *  The final two bytes are to be set by the user and represent a way for
 *  users to specify an order within a single transaction.
 */
public class Versionstamp implements Comparable<Versionstamp> {
	public static final int LENGTH = 12;
	protected static final byte[] UNSET_GLOBAL_VERSION = {(byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
														  (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff};

	private boolean complete;
	public byte[] versionBytes;

	/**
	 * From a byte array, unpack the user version starting at the given position.
	 *  This assumes the bytes are serialized in the array in the same way in
	 *  the given array as this class would serialize them, i.e., in big-endian
	 *  order as an unsigned short.
	 *
	 * @param bytes byte array including user version
	 * @param pos starting position of user version
	 * @return the unpacked user version included in the array
	 */
	public static int unpackUserVersion(byte[] bytes, int pos) {
		return ((int)bytes[pos] & 0xff << 8) | ((int)bytes[pos + 1] & 0xff);
	}

	/**
	 * Creates a {@code Versionstamp} instance based on the given byte array
	 *  representation. This follows the same format as that used by
	 *  the main constructor, but the completeness of the {@code Versionstamp}
	 *  is instead automatically determined by comparing its global version
	 *  with the value used to indicate an unset global version.
	 *
	 * @param versionBytes byte array representation of {@code Versionstamp}
	 * @return equivalent instantiated {@code Versionstamp} object
	 */
	public static Versionstamp fromBytes(byte[] versionBytes) {
		if(versionBytes.length != LENGTH) {
			throw new IllegalArgumentException("Versionstamp bytes must have length " + LENGTH);
		}
		boolean complete = false;
		for(int i = 0; i < UNSET_GLOBAL_VERSION.length; i++) {
			if(versionBytes[i] != UNSET_GLOBAL_VERSION[i]) {
				complete = true;
			}
		}
		return new Versionstamp(complete, versionBytes);
	}

	/**
	 * Creates an incomplete {@code Versionstamp} instance with the given
	 *  user version. The provided user version must fit within an unsigned
	 *  short. When converted into a byte array, the bytes for the transaction
	 *  version will be filled in with the
	 *
	 * @param userVersion intra-transaction portion of version (set by user code)
	 * @return an incomplete {@code Versionstamp} with the given user version
	 */
	public static Versionstamp incomplete(int userVersion) {
		if(userVersion < 0 || userVersion > 0xffff) {
			throw new IllegalArgumentException("Local version must fit in unsigned short");
		}
		ByteBuffer bb = ByteBuffer.allocate(LENGTH).order(ByteOrder.BIG_ENDIAN);
		bb.put(UNSET_GLOBAL_VERSION);
		bb.putShort((short)userVersion);
		return new Versionstamp(false, bb.array());
	}

	/**
	 * Creates a complete {@code Versionstamp} instance with the given
	 *  transaction and user versions. The provided transaction version must have
	 *  exactly 10 bytes, and the user version must fit within an unsigned
	 *  short.
	 *
	 * @param trVersion  inter-transaction portion of version (set by resolver)
	 * @param userVersion intra-transaction portion of version (set by user code)
	 * @return a complete {@code Versionstamp} assembled from the given parts
	 */
	public static Versionstamp complete(byte[] trVersion, int userVersion) {
		if(trVersion.length != UNSET_GLOBAL_VERSION.length) {
			throw new IllegalArgumentException("Global version has invalid length " + trVersion.length);
		}
		if(userVersion < 0 || userVersion > 0xffff) {
			throw new IllegalArgumentException("Local version must fit in unsigned short");
		}
		ByteBuffer bb = ByteBuffer.allocate(LENGTH).order(ByteOrder.BIG_ENDIAN);
		bb.put(trVersion);
		bb.putShort((short)userVersion);
		return new Versionstamp(true, bb.array());
	}

	/**
	 * Create a <code>Versionstamp</code> instance from a byte array.
	 *  The byte array should have length {@value LENGTH}, the first
	 *  10 of which represent the transaction version of
	 *  an operation and the last two of which are set per transaction
	 *  to impose a global ordering on all versionstamps.
	 *
	 *  If the transaction version is not yet known (because the commit has not
	 *  been performed), set <code>complete</code> to <code>false</code>
	 *  and the first 10 bytes of <code>versionBytes</code>
	 *  to any value. Otherwise, <code>complete</code> should be set to
	 *  <code>true</code>.
	 *
	 * @param complete whether the transaction version has been set
	 * @param versionBytes byte array representing this <code>Versionstamp</code> information
	 */
	public Versionstamp(boolean complete, byte[] versionBytes) {
		if (versionBytes.length != LENGTH) {
			throw new IllegalArgumentException("Versionstamp bytes must have length " + LENGTH);
		}
		this.complete = complete;
		this.versionBytes = versionBytes;
	}

	/**
	 * Whether this <code>Versionstamp</code>'s transaction version is
	 *  meaningful. The transaction resolver will assign each transaction
	 *  a different transaction version. This <code>Versionstamp</code> is
	 *  considered complete if some version has been previously received
	 *  from the transaction resolver and used to construct this
	 *  object. If the commit is still yet to occur, the <code>Versionstamp</code>
	 *  is considered incomplete. If one uses this class with
	 *  our {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
	 *  or {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_VALUE SET_VERSIONSTAMPED_VALUE}
	 *  mutations, then the appropriate bytes should be filled in within
	 *  the database during the commit.
	 *
	 * @return whether the transaction version has been set
	 */
	public boolean isComplete() {
		return complete;
	}

	/**
	 * Retrieve a byte-array representation of this <code>Versionstamp</code>.
	 *  This representation can then be serialized and added to the database.
	 *  If this <code>Versionstamp</code> is not complete, the first
	 *  10 bytes (representing the transaction version) will
	 *  not be meaningful and one should probably use either the
	 *  {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
	 *  or {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_VALUE SET_VERSIONSTAMPED_VALUE}
	 *  mutations.
	 *
	 * <b>Warning:</b> For performance reasons, this method does not create a copy of
	 *  its underlying data array. As a result, it is dangerous to modify the
	 *  return value of this function and should not be done in most circumstances.
	 *
	 * @return byte representation of this <code>Versionstamp</code>
	 */
	public byte[] getBytes() {
		return versionBytes;
	}

	/**
	 * Retrieve the portion of this <code>Versionstamp</code> that is set by
	 *  the transaction resolver. These 10 bytes are what provide an ordering
	 *  between different commits.
	 *
	 * @return transaction version of this <code>Versionstamp</code>
	 */
	public byte[] getTransactionVersion() {
		byte[] ret = new byte[LENGTH - 2];
		System.arraycopy(versionBytes, 0, ret, 0, ret.length);
		return ret;
	}

	/**
	 * Retrieve the portion of this <code>Versionstamp</code> that is set
	 *  by the user. This integer is what provides an ordering within
	 *  one commit.
	 *
	 * @return user version of this <code>Versionstamp</code>
	 */
	public int getUserVersion() {
		return unpackUserVersion(versionBytes, LENGTH - 2);
	}

	@Override
	public String toString() {
		if(complete) {
			return "Versionstamp(" + ByteArrayUtil.printable(versionBytes) + ")";
		} else {
			return "Versionstamp(<incomplete> " + getUserVersion() + ")";
		}
	}

	/**
	 * Compares two {@code Versionstamp} instances in a manner consistent with their
	 *  key order when serialized in the database as keys. The rules for comparison are:
	 *
	 * <ul>
	 *     <li>All complete {@code Versionstamp}s sort before incomplete {@code Versionstamp}s</li>
	 *     <li>
	 *         Two complete {@code Versionstamp}s will sort based on unsigned lexicographic comparison
	 *         of their byte representations.
	 *     </li>
	 *     <li>Two incomplete {@code Versionstamp}s will sort based on their user versions.</li>
	 * </ul>
     *
	 * @param other {@code Versionstamp} instance to compare against
	 * @return -1 if this {@code Versionstamp} is smaller than {@code other}, 1 if it is bigger, and
	 *  0 if it is equal
	 */
	@Override
	public int compareTo(Versionstamp other) {
		if(complete && other.complete) {
			return ByteArrayUtil.compareUnsigned(this.versionBytes, other.versionBytes);
		} else if(complete) {
			// The other is incomplete and thus greater.
			return -1;
		} else if(other.complete) {
			// This instance is incomplete and thus greater.
			return 1;
		} else {
			return TupleUtil.compareIntegers(getUserVersion(), other.getUserVersion());
		}
	}

	/**
	 * Check another object for equality. This will return <code>true</code> if and only
	 *  if the <code>o</code> parameter is of type <code>Versionstamp</code> and has
	 *  the same completion status and byte representation.
	 *
	 * @param o object to compare for equality
	 * @return whether the passed object is an equivalent <code>Versionstamp</code>
	 */
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (this == o) {
			return true;
		}
		if (!(o instanceof Versionstamp)) {
			return false;
		}
		Versionstamp that = (Versionstamp)o;
		return this.isComplete() == that.isComplete() && Arrays.equals(this.getBytes(), that.getBytes());
	}

	/**
	 * Hash code for this <code>Versionstamp</code>. It is based off of the hash
	 *  code for the underlying data array.
	 *
	 * @return hash-table compliant hash code for this instance
	 */
	public int hashCode() {
		return Arrays.hashCode(getBytes());
	}
}
