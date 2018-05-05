/*
 * Directory.java
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

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.TransactionContext;

/**
 * Represents a directory in the {@code DirectoryLayer}. A {@code Directory} stores the path
 * at which it is located and the layer that was used to create it.
 *
 * The {@code Directory} interface contains methods to operate on itself and its
 * subdirectories.
 */
public interface Directory {

	/**
	 * Gets the path represented by this {@code Directory}.
	 *
	 * @return this {@code Directory}'s path
	 */
	List<String> getPath();

	/**
	 * Gets the layer byte string that was stored when this {@code Directory}
	 * was created.
	 *
	 * @return this {@code Directory}'s layer byte string
	 */
	byte[] getLayer();

	/**
	 * Get the {@link DirectoryLayer} that was used to create this {@code Directory}.
	 *
	 * @return the {@link DirectoryLayer} that created this {@link Directory}
	 */
	DirectoryLayer getDirectoryLayer();

	/**
	 * Creates or opens the subdirectory of this {@code Directory} located at {@code subpath}
	 * (creating parent directories, if necessary).
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set to the created or opened {@link DirectorySubspace}
	 */
	default CompletableFuture<DirectorySubspace> createOrOpen(TransactionContext tcx, List<String> subpath) {
		return createOrOpen(tcx, subpath, EMPTY_BYTES);
	}

	/**
	 * Creates or opens the subdirectory of this {@code Directory} located at {@code subpath}
	 * (creating parent directories, if necessary). If the directory is new, then the {@code layer}
	 * byte string will be recorded as its layer.  If the directory already exists, the {@code layer}
	 * byte string will be compared against the {@code layer} set when the directory was created.
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link MismatchedLayerException} - if the directory has already been created with a different {@code layer} byte string</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @param layer a {@code byte[]} specifying a layer to set on a new directory or check for on an existing directory
	 * @return a {@link CompletableFuture} which will be set to the created or opened {@link DirectorySubspace}
	 */
	CompletableFuture<DirectorySubspace> createOrOpen(TransactionContext tcx, List<String> subpath, byte[] layer);

	/**
	 * Opens the subdirectory of this {@code Directory} located at {@code subpath}.
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if the directory does not exist</li>
	 * </ul>
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set to the opened {@link DirectorySubspace}
	 */
	default CompletableFuture<DirectorySubspace> open(ReadTransactionContext tcx, List<String> subpath) {
		return open(tcx, subpath, EMPTY_BYTES);
	}

	/**
	 * Opens the subdirectory of this {@code Directory} located at {@code subpath}.
	 * The {@code layer} byte string will be compared against the {@code layer} set when
	 * the directory was created.
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link MismatchedLayerException} - if the directory was created with a different {@code layer} byte string</li>
	 *   <li>{@link NoSuchDirectoryException} - if the directory does not exist</li>
	 * </ul>
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @param layer a {@code byte[]} specifying the expected layer
	 * @return a {@link CompletableFuture} which will be set to the opened {@link DirectorySubspace}
	 */
	CompletableFuture<DirectorySubspace> open(ReadTransactionContext tcx, List<String> subpath, byte[] layer);

	/**
	 * Creates a subdirectory of this {@code Directory} located at {@code subpath}
	 * (creating parent directories if necessary).
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryAlreadyExistsException} - if the given directory already exists</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set to the created {@link DirectorySubspace}
	 */
	default CompletableFuture<DirectorySubspace> create(TransactionContext tcx, List<String> subpath) {
		return create(tcx, subpath, EMPTY_BYTES);
	}

	/**
	 * Creates a subdirectory of this {@code Directory} located at {@code subpath}
	 * (creating parent directories if necessary). The {@code layer} byte string will be recorded as
	 * the new directory's layer and checked by future calls to {@link #open(ReadTransactionContext, List, byte[])}.
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryAlreadyExistsException} - if the given directory already exists</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @param layer a {@code byte[]} specifying a layer to set for the directory
	 * @return a {@link CompletableFuture} which will be set to the created {@link DirectorySubspace}
	 */
	default CompletableFuture<DirectorySubspace> create(TransactionContext tcx, List<String> subpath, byte[] layer) {
		return create(tcx, subpath, layer, null);
	}

	/**
	 * Creates a subdirectory of this {@code Directory} located at {@code subpath}
	 * (creating parent directories if necessary). The {@code layer} byte string will be recorded as
	 * the new directory's layer and checked by future calls to {@link #open(ReadTransactionContext, List, byte[])}.
	 * The specified {@code prefix} will be used for this directory's contents instead of allocating a
	 * prefix automatically.
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryAlreadyExistsException} - if the given directory already exists</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @param layer a {@code byte[]} specifying a layer to set for the directory
	 * @param prefix a {@code byte[]} specifying the key prefix to use for the directory's contents
	 * @return a {@link CompletableFuture} which will be set to the created {@link DirectorySubspace}
	 */
	CompletableFuture<DirectorySubspace> create(TransactionContext tcx, List<String> subpath, byte[] layer, byte[] prefix);

	/**
	 * Moves this {@code Directory} to the specified {@code newAbsolutePath}.
	 * <p>
	 *   There is no effect on the physical prefix of the given directory, or on clients that already
	 *   have the directory open.
	 * </P>
	 * <p>
	 *   It is invalid to move a directory to:
	 * </p>
	 * <ul>
	 *   <li>A location where a directory already exists</li>
	 *   <li>A location whose parent does not exist</li>
	 *   <li>A subdirectory of itself</li>
	 *   <li>A different partition</li>
	 * </ul>
	 *
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if this {@code Directory} doesn't exist</li>
	 *   <li>{@link DirectoryAlreadyExistsException} - if a directory already exists at {@code newAbsolutePath}</li>
	 *   <li>{@link DirectoryMoveException} - if an invalid move location is specified</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param newAbsolutePath a {@code List<String>} specifying the new absolute path for this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set to the {@link DirectorySubspace} for this {@code Directory}
	 * at its new location.
	 */
	CompletableFuture<DirectorySubspace> moveTo(TransactionContext tcx, List<String> newAbsolutePath);

	/**
	 * Moves the subdirectory of this {@code Directory} located at {@code oldSubpath} to {@code newSubpath}.
	 *
	 * <p>
	 *   There is no effect on the physical prefix of the given directory, or on clients that already
	 *   have the directory open.
	 * </p>
	 * <p>
	 *   It is invalid to move a directory to:
	 * </p>
	 * <ul>
	 *   <li>A location where a directory already exists</li>
	 *   <li>A location whose parent does not exist</li>
	 *   <li>A subdirectory of itself</li>
	 *   <li>A different partition</li>
	 * </ul>
	 *
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if no {@code Directory} exists at {@code oldSubpath}</li>
	 *   <li>{@link DirectoryAlreadyExistsException} - if a directory already exists at {@code newSubpath}</li>
	 *   <li>{@link DirectoryMoveException} - if an invalid move location is specified</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param oldSubpath a {@code List<String>} specifying the subpath of the directory to move
	 * @param newSubpath a {@code List<String>} specifying the subpath to move to
	 * @return a {@link CompletableFuture} which will be set to the {@link DirectorySubspace} for this {@code Directory}
	 * at its new location.
	 */
	CompletableFuture<DirectorySubspace> move(TransactionContext tcx, List<String> oldSubpath, List<String> newSubpath);

	/**
	 * Removes this {@code Directory} and all of its subdirectories, as well as all of their contents.
	 * This should not be called on the root directory, or it will result in the returned future being
	 * set to a {@link DirectoryException}.
	 *
	 * <p><i>
	 *   Warning: Clients that have already opened the directory might
	 *   still insert data into its contents after it is removed.
	 * </i></p>
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if this {@code Directory} doesn't exist</li>
	 *   <li>{@link DirectoryException} - if this is called on the root directory</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @return a {@link CompletableFuture} which will be set once this {@code Directory} has been removed
	 */
	default CompletableFuture<Void> remove(TransactionContext tcx) {
		return remove(tcx, EMPTY_PATH);
	}

	/**
	 * Removes the subdirectory of this {@code Directory} located at {@code subpath} and all of its subdirectories,
	 * as well as all of their contents.
	 *
	 * <p><i>
	 *   Warning: Clients that have already opened the directory might
	 *   still insert data into its contents after it is removed.
	 * </i></p>
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if no directory exists at {@code subpath}</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set once the {@code Directory} has been removed
	 */
	CompletableFuture<Void> remove(TransactionContext tcx, List<String> subpath);

	/**
	 * Removes this {@code Directory} and all of its subdirectories, as well as all of their contents.
	 *
	 * <p><i>
	 *   Warning: Clients that have already opened the directory might
	 *   still insert data into its contents after it is removed.
	 * </i></p>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @return a {@link CompletableFuture} which will be set to true once this {@code Directory} has been removed,
	 * or false if it didn't exist.
	 */
	default CompletableFuture<Boolean> removeIfExists(TransactionContext tcx) {
		return removeIfExists(tcx, EMPTY_PATH);
	}

	/**
	 * Removes the subdirectory of this {@code Directory} located at {@code subpath} and all of its subdirectories,
	 * as well as all of their contents.
	 *
	 * <p><i>
	 *   Warning: Clients that have already opened the directory might
	 *   still insert data into its contents after it is removed.
	 * </i></p>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set to true once the {@code Directory} has been removed,
	 * or false if it didn't exist.
	 */
	CompletableFuture<Boolean> removeIfExists(TransactionContext tcx, List<String> subpath);

	/**
	 * List the subdirectories of this directory.
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if this {@code Directory} doesn't exists</li>
	 * </ul>
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @return a {@link CompletableFuture} which will be set to a {@code List<String>} of names of the subdirectories
	 * of this {@code Directory}. Each name is a unicode string representing the last component of a
	 * subdirectory's path.
	 */
	default CompletableFuture<List<String>> list(ReadTransactionContext tcx) {
		return list(tcx, EMPTY_PATH);
	}

	/**
	 * List the subdirectories of this directory at a given {@code subpath}.
	 *
	 * <p>The returned {@link CompletableFuture} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if no directory exists at {@code subpath}</li>
	 * </ul>
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set to a {@code List<String>} of names of the subdirectories
	 * of the directory at {@code subpath}. Each name is a unicode string representing the last component
	 * of a subdirectory's path.
	 */
	CompletableFuture<List<String>> list(ReadTransactionContext tcx, List<String> subpath);

	/**
	 * Checks if this {@code Directory} exists.
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @return a {@link CompletableFuture} which will be set to {@code true} if this {@code Directory} exists, or {@code false} if it
	 * doesn't
	 */
	default CompletableFuture<Boolean> exists(ReadTransactionContext tcx) {
		return exists(tcx, EMPTY_PATH);
	}

	/**
	 * Checks if the subdirectory of this {@code Directory} located at {@code subpath} exists.
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param subpath a {@code List<String>} specifying a subpath of this {@code Directory}
	 * @return a {@link CompletableFuture} which will be set to {@code true} if the specified subdirectory exists, or {@code false} if it
	 * doesn't
	 */
	CompletableFuture<Boolean> exists(ReadTransactionContext tcx, List<String> subpath);
}
