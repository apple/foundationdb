/*
 * DirectoryPartition.java
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

package com.apple.cie.foundationdb.directory;

import static com.apple.cie.foundationdb.directory.DirectoryLayer.DEFAULT_NODE_SUBSPACE_PREFIX;

import java.util.List;

import com.apple.cie.foundationdb.subspace.Subspace;
import com.apple.cie.foundationdb.Range;
import com.apple.cie.foundationdb.tuple.ByteArrayUtil;
import com.apple.cie.foundationdb.tuple.Tuple;

/**
 * A {@code DirectoryPartition} is a {@link DirectorySubspace} whose prefix is prepended to all of its descendant directories' prefixes. 
 *
 * <p>
 *   A {@code DirectoryPartition} cannot be used as a {@link Subspace}. Instead, you must create at least one subdirectory to store
 *   content.
 * </p>
 *
 * For general guidance on partition usage, see 
 * <a href="/documentation/developer-guide.html#directory-partitions" target="_blank">The Developer Guide</a>.
 */
class DirectoryPartition extends DirectorySubspace {

	private final DirectoryLayer parentDirectoryLayer;

	DirectoryPartition(List<String> path, byte[] prefix, DirectoryLayer parentDirectoryLayer) {
		super(path,
				prefix,
				new DirectoryLayer(new Subspace(ByteArrayUtil.join(prefix, DEFAULT_NODE_SUBSPACE_PREFIX)), new Subspace(prefix)),
				DirectoryLayer.PARTITION_LAYER);

		getDirectoryLayer().setPath(path);
		this.parentDirectoryLayer = parentDirectoryLayer;
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public Subspace get(Object o) {
		throw new UnsupportedOperationException("Cannot open subspace in the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public Subspace get(Tuple name) {
		throw new UnsupportedOperationException("Cannot open subspace in the root of a directory partition.");
	}

	/**
	 * Raises an exception because DirectoryPartition cannot be used as a Subspace.
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public byte[] getKey() {
		throw new UnsupportedOperationException("Cannot get key for the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public byte[] pack() {
		throw new UnsupportedOperationException("Cannot pack keys using the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public byte[] pack(Object o) {
		throw new UnsupportedOperationException("Cannot pack keys using the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public byte[] pack(Tuple tuple) {
		throw new UnsupportedOperationException("Cannot pack keys using the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public Tuple unpack(byte[] key) {
		throw new UnsupportedOperationException("Cannot unpack keys using the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public Range range() {
		throw new UnsupportedOperationException("Cannot get range for the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public Range range(Tuple tuple) {
		throw new UnsupportedOperationException("Cannot get range for the root of a directory partition.");
	}

	/** 
	 * Raises an exception because DirectoryPartition cannot be used as a Subspace.
	 *
	 * @throws UnsupportedOperationException
	 */
	@Override
	public boolean contains(byte[] key) {
		throw new UnsupportedOperationException("Cannot check whether a key belongs to the root of a directory partition.");
	}

    /**
    * Raises an exception because DirectoryPartition cannot be used as a Subspace.
    *
    * @throws UnsupportedOperationException
    */
	@Override
	public Subspace subspace(Tuple tuple) {
		throw new UnsupportedOperationException("Cannot open subspace in the root of a directory partition.");
	}

	@Override
	DirectoryLayer getLayerForPath(List<String> path) {
		if(path.size() == 0)
			return parentDirectoryLayer;
		else
			return getDirectoryLayer();
	}

	/**
	 * Returns whether this {@code DirectoryPartition} is equal to {@code rhs}.
	 * Two {@code DirectoryPartition}s are equal if they were created by the same
	 * {@link DirectoryLayer} and have the same path, layer, and subspace prefix.
	 *
	 * @param rhs the {@code} Object to test for equality
	 * @return true if this is equal to {@code rhs}
	 */
    @Override
    public boolean equals(Object rhs) {
        if(this == rhs) {
            return true;
        }
        if(rhs == null || getClass() != rhs.getClass()) {
            return false;
        }

        DirectoryPartition other = (DirectoryPartition)rhs;
        return (getPath() == other.getPath() || getPath() == other.getPath()) && 
				parentDirectoryLayer.equals(other.parentDirectoryLayer) &&
				super.equals(rhs);
    }
}
