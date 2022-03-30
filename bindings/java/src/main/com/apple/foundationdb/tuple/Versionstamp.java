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
 * Used to represent values written by versionstamp operations with a {@link Tuple}.
 *  This wraps a single array which should contain twelve bytes. The first ten bytes
 *  are the "transaction" version, and they are usually assigned by the database
 *  in such a way that all transactions receive a different version that is consistent
 *  with a serialization order of the transactions within the database. (One can
 *  use the {@link com.apple.foundationdb.Transaction#getVersionstamp() Transaction.getVersionstamp()}
 *  method to retrieve this version from a {@code Transaction}.) This also implies that the
 *  transaction version of newly committed transactions will be monotonically increasing
 *  over time. The final two bytes are the "user" version and should be set by the client.
 *  This allows the user to use this class to impose a total order of items across multiple
 *  transactions in the database in a consistent and conflict-free way. The user can elect to
 *  ignore this parameter by instantiating the class with the parameterless {@link #incomplete() incomplete()}
 *  and one-parameter {@link #complete(byte[]) complete} static initializers. If they do so,
 *  then versions are written with a default (constant) user version.
 *
 * <p>
 * All {@code Versionstamp}s can exist in one of two states: "incomplete" and "complete".
 *  An "incomplete" {@code Versionstamp} is a {@code Versionstamp} that has not been
 *  initialized with a meaningful transaction version. For example, this might be used
 *  with a {@code Versionstamp} that one wants to fill in with the current transaction's
 *  version information. A "complete" {@code Versionstamp}, in contradistinction, is one
 *  that <i>has</i> been assigned a meaningful transaction version. This is usually the
 *  case if one is reading back a {@code Versionstamp} from the database.
 * </p>
 *
 * <p>
 * Example usage might be to do something like the following:
 * </p>
 *
 * <pre>
 * <code>
 *  CompletableFuture&lt;byte[]&gt; trVersionFuture = db.run((Transaction tr) -&gt; {
 *       // The incomplete Versionstamp will be overwritten with tr's version information when committed.
 *       Tuple t = Tuple.from("prefix", Versionstamp.incomplete());
 *       tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, t.packWithVersionstamp(), new byte[0]);
 *       return tr.getVersionstamp();
 *   });
 *
 *   byte[] trVersion = trVersionFuture.get();
 *
 *   Versionstamp v = db.run((Transaction tr) -&gt; {
 *       Subspace subspace = new Subspace(Tuple.from("prefix"));
 *       byte[] serialized = tr.getRange(subspace.range(), 1).iterator().next().getKey();
 *       Tuple t = subspace.unpack(serialized);
 *       return t.getVersionstamp(0);
 *   });
 *
 *   assert v.equals(Versionstamp.complete(trVersion));
 * </code>
 * </pre>
 *
 * <p>
 * Here, an incomplete {@code Versionstamp} is packed and written to the database with
 *  the {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
 *  {@code MutationType}. After committing, we then attempt to read back the same key that
 *  we just wrote. Then we verify the invariant that the deserialized {@link Versionstamp} is
 *  the same as a complete {@code Versionstamp} instance created from the first transaction's
 *  version information.
 * </p>
 */
public class Versionstamp implements Comparable<Versionstamp> {
	/**
	 * Length of a serialized {@code Versionstamp} instance when converted into a byte array.
	 */
	public static final int LENGTH = 12;
	private static final byte[] UNSET_TRANSACTION_VERSION = {(byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff,
	                                                         (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff};

	private final boolean complete;
	private final byte[] versionBytes;

	/**
	 * From a byte array, unpack the user version starting at the given position.
	 *  This assumes that the bytes are stored in big-endian order as an unsigned
	 *  short, which is the way the user version is serialized in packed {@code Versionstamp}s.
	 *
	 * @param bytes byte array including user version
	 * @param pos starting position of user version
	 * @return the unpacked user version included in the array
	 */
	public static int unpackUserVersion(byte[] bytes, int pos) {
		return (((int)bytes[pos] & 0xff) << 8) | ((int)bytes[pos + 1] & 0xff);
	}

	/**
	 * Creates a {@code Versionstamp} instance based on the given byte array
	 *  representation. This follows the same format as that used by
	 *  the main constructor, but the completeness of the {@code Versionstamp}
	 *  is instead automatically determined by comparing its transaction version
	 *  with the value used to indicate an unset transaction version.
	 *
	 * @param versionBytes byte array representation of {@code Versionstamp}
	 * @return equivalent instantiated {@code Versionstamp} object
	 */
	public static Versionstamp fromBytes(byte[] versionBytes) {
		if(versionBytes.length != LENGTH) {
			throw new IllegalArgumentException("Versionstamp bytes must have length " + LENGTH);
		}
		boolean complete = false;
		for(int i = 0; i < UNSET_TRANSACTION_VERSION.length; i++) {
			if(versionBytes[i] != UNSET_TRANSACTION_VERSION[i]) {
				complete = true;
			}
		}
		return new Versionstamp(complete, versionBytes);
	}

	/**
	 * Creates an incomplete {@code Versionstamp} instance with the given
	 *  user version. The provided user version must fit within an unsigned
	 *  short. When converted into a byte array, the bytes for the transaction
	 *  version will be filled in with dummy bytes to be later filled
	 *  in at transaction commit time.
	 *
	 * @param userVersion intra-transaction portion of version (set by user code)
	 * @return an incomplete {@code Versionstamp} with the given user version
	 */
	public static Versionstamp incomplete(int userVersion) {
		if(userVersion < 0 || userVersion > 0xffff) {
			throw new IllegalArgumentException("Local version must fit in unsigned short");
		}
		ByteBuffer bb = ByteBuffer.allocate(LENGTH).order(ByteOrder.BIG_ENDIAN);
		bb.put(UNSET_TRANSACTION_VERSION);
		bb.putShort((short)userVersion);
		return new Versionstamp(false, bb.array());
	}

	/**
	 * Creates an incomplete {@code Versionstamp} instance with the default user
	 *  version. When converted into a byte array, the bytes for the transaction
	 *  version will be filled in with dummy bytes to be later filled in at
	 *  transaction commit time. If multiple keys are created using the returned
	 *  {@code Versionstamp} within the same transaction, then all of those
	 *  keys will have the same version, but it will provide an ordering between
	 *  different transactions if that is all that is required.
	 *
	 * @return an incomplete {@code Versionstamp} with the default user version
	 */
	public static Versionstamp incomplete() {
		return incomplete(0);
	}

	/**
	 * Creates a complete {@code Versionstamp} instance with the given
	 *  transaction and user versions. The provided transaction version must have
	 *  exactly 10 bytes, and the user version must fit within an unsigned
	 *  short.
	 *
	 * @param trVersion  inter-transaction portion of version (set by the database)
	 * @param userVersion intra-transaction portion of version (set by user code)
	 * @return a complete {@code Versionstamp} assembled from the given parts
	 */
	public static Versionstamp complete(byte[] trVersion, int userVersion) {
		if(trVersion.length != UNSET_TRANSACTION_VERSION.length) {
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
	 * Creates a complete {@code Versionstamp} instance with the given
	 *  transaction and default user versions. The provided transaction version
	 *  must have exactly 10 bytes.
	 *
	 * @param trVersion  inter-transaction portion of version (set by the database)
	 * @return a complete {@code Versionstamp} assembled from the given transaction
	 * 	version and the default user version
	 */
	public static Versionstamp complete(byte[] trVersion) {
		return complete(trVersion, 0);
	}

	private Versionstamp(boolean complete, byte[] versionBytes) {
		if (versionBytes.length != LENGTH) {
			throw new IllegalArgumentException("Versionstamp bytes must have length " + LENGTH);
		}
		this.complete = complete;
		this.versionBytes = versionBytes;
	}

	/**
	 * Whether this {@code Versionstamp}'s transaction version is
	 *  meaningful. The database will assign each transaction a different transaction
	 *  version. A {@code Versionstamp} is considered to be "complete" if its
	 *  transaction version is one of those database-assigned versions rather than
	 *  just dummy bytes. If one uses this class with our
	 *  {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
	 *  mutation, then the appropriate bytes will be filled in within the database
	 *  during a successful commit.
	 *
	 * @return whether the transaction version has been set
	 */
	public boolean isComplete() {
		return complete;
	}

	/**
	 * Retrieve a byte-array representation of this {@code Versionstamp}.
	 *  This representation can then be serialized and added to the database.
	 *  If this {@code Versionstamp} is not complete, the first 10 bytes (representing the
	 *  transaction version) will not be meaningful and one should probably use with the
	 *  {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
	 *  mutation.
	 *
	 * <b>Warning:</b> For performance reasons, this method does not create a copy of
	 *  its underlying data array. As a result, it is dangerous to modify the
	 *  return value of this function.
	 *
	 * @return byte representation of this <code>Versionstamp</code>
	 */
	public byte[] getBytes() {
		return versionBytes;
	}

	/**
	 * Retrieve the portion of this <code>Versionstamp</code> that is set by
	 *  the database. These 10 bytes are what provide an ordering between different commits.
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
	 *  a single commit.
	 *
	 * @return user version of this <code>Versionstamp</code>
	 */
	public int getUserVersion() {
		return unpackUserVersion(versionBytes, LENGTH - 2);
	}

	/**
	 * Generate a human-readable representation of this {@code Versionstamp}. It contains
	 *  information as to whether this {@code Versionstamp} is incomplete or not, what
	 *  its transaction version is (if the {@code Versionstamp} is complete), and what its
	 *  user version is.
	 *
	 * @return a human-readable representation of this {@code Versionstamp}
	 */
	@Override
	public String toString() {
		if(complete) {
			return "Versionstamp(" + ByteArrayUtil.printable(getTransactionVersion()) + " " + getUserVersion() + ")";
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
			return Integer.compare(getUserVersion(), other.getUserVersion());
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
