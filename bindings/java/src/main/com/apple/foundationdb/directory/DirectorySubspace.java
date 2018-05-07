/*
 * DirectorySubspace.java
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

package com.apple.foundationdb.directory;

import static com.apple.foundationdb.directory.DirectoryLayer.EMPTY_BYTES;
import static com.apple.foundationdb.directory.DirectoryLayer.EMPTY_PATH;
import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;

/**
 *  A DirectorySubspace represents the <i>contents</i> of a directory, but it
 *  also remembers the path with which it was opened and offers convenience
 *  methods to operate on the directory at that path.
 *
 * <p>
 *     An instance of DirectorySubspace can be used for all the usual subspace
 *     operations. It can also be used to operate on the directory with which
 *     it was opened.
 * </p>
 */
public class DirectorySubspace extends Subspace implements Directory {
	private final List<String> path;
	private final byte[] layer;
	private final DirectoryLayer directoryLayer;

	DirectorySubspace(List<String> path, byte[] prefix, DirectoryLayer directoryLayer) {
		this(path, prefix, directoryLayer, EMPTY_BYTES);
	}

	DirectorySubspace(List<String> path, byte[] prefix, DirectoryLayer directoryLayer, byte[] layer) {
		super(prefix);
		this.path = path;
		this.layer = layer;
		this.directoryLayer = directoryLayer;
	}

	/**
	 * @return a printable representation of this {@code DirectorySubspace}
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName() + '(' + DirectoryUtil.pathStr(path) + ", " + printable(getKey()) + ')';
	}

	/**
	 * Returns whether this {@code DirectorySubspace} is equal to {@code rhs}.
	 * Two {@code DirectorySubspace}s are equal if they were created by the same
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
		DirectorySubspace other = (DirectorySubspace)rhs;
		return (path == other.path || path.equals(other.path)) &&
			   Arrays.equals(layer, other.layer) &&
			   directoryLayer.equals(other.directoryLayer) &&
			   super.equals(rhs);
	}

	/**
	 * Computes a hash code compatible with the {@link #equals(Object) equals()} method of
	 * this class. In particular, it will produce a hash code that is based off of the hashes
	 * of its path, its layer, and its subspace prefix.
	 *
	 * @return a hash compatible with this class's {@code equals()} method
	 */
	@Override
	public int hashCode() {
		return path.hashCode() ^ (Arrays.hashCode(layer) * 1153) ^ (directoryLayer.hashCode() * 929) ^ (super.hashCode() * 419);
	}

	@Override
	public List<String> getPath() {
		return Collections.unmodifiableList(path);
	}

	@Override
	public byte[] getLayer() {
		return Arrays.copyOf(layer, layer.length);
	}

	@Override
	public DirectoryLayer getDirectoryLayer() {
		return directoryLayer;
	}

	@Override
	public CompletableFuture<DirectorySubspace> createOrOpen(TransactionContext tcx, List<String> subpath, byte[] otherLayer) {
		return directoryLayer.createOrOpen(tcx, getPartitionSubpath(subpath), otherLayer);
	}

	@Override
	public CompletableFuture<DirectorySubspace> open(ReadTransactionContext tcx, List<String> subpath, byte[] otherLayer) {
		return directoryLayer.open(tcx, getPartitionSubpath(subpath), otherLayer);
	}

	@Override
	public CompletableFuture<DirectorySubspace> create(TransactionContext tcx, List<String> subpath, byte[] otherLayer, byte[] prefix) {
		return directoryLayer.create(tcx, getPartitionSubpath(subpath), otherLayer, prefix);
	}

	@Override
	public CompletableFuture<List<String>> list(ReadTransactionContext tcx, List<String> subpath) {
		return directoryLayer.list(tcx, getPartitionSubpath(subpath));
	}

	@Override
	public CompletableFuture<DirectorySubspace> move(TransactionContext tcx, List<String> oldSubpath, List<String> newSubpath) {
		return directoryLayer.move(tcx, getPartitionSubpath(oldSubpath), getPartitionSubpath(newSubpath));
	}

	@Override
	public CompletableFuture<DirectorySubspace> moveTo(TransactionContext tcx, List<String> newAbsolutePath) {
		DirectoryLayer dir = getLayerForPath(EMPTY_PATH);
		int partitionLen = dir.getPath().size();
		List<String> partitionPath = newAbsolutePath.subList(0, Math.min(newAbsolutePath.size(), partitionLen));
		if(!partitionPath.equals(dir.getPath()))
			throw new DirectoryMoveException("Cannot move between partitions", path, newAbsolutePath);

		return dir.move(tcx,
						getPartitionSubpath(EMPTY_PATH, dir),
						newAbsolutePath.subList(partitionLen, newAbsolutePath.size()));
	}

	@Override
	public CompletableFuture<Void> remove(TransactionContext tcx, List<String> subpath) {
		DirectoryLayer dir = getLayerForPath(subpath);
		return dir.remove(tcx, getPartitionSubpath(subpath, dir));
	}

	@Override
	public CompletableFuture<Boolean> removeIfExists(TransactionContext tcx, List<String> subpath) {
		DirectoryLayer dir = getLayerForPath(subpath);
		return dir.removeIfExists(tcx, getPartitionSubpath(subpath, dir));
	}

	@Override
	public CompletableFuture<Boolean> exists(ReadTransactionContext tcx, List<String> subpath) {
		DirectoryLayer dir = getLayerForPath(subpath);
		return dir.exists(tcx, getPartitionSubpath(subpath, dir));
	}

	private List<String> getPartitionSubpath(List<String> path) {
		return getPartitionSubpath(path, directoryLayer);
	}

	private List<String> getPartitionSubpath(List<String> path, DirectoryLayer directoryLayer) {
		return PathUtil.join(this.path.subList(directoryLayer.getPath().size(), this.path.size()), path);
	}

	/**
	 * Called by all functions that could operate on this subspace directly (moveTo, remove, removeIfExists, exists).
	 * Subclasses can chooose to return a different directory layer to use for the operation if path is in fact empty.
	 */
	DirectoryLayer getLayerForPath(List<String> path) {
		return directoryLayer;
	}
}
