/*
 * DirectoryLayer.java
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

import static com.apple.foundationdb.tuple.ByteArrayUtil.join;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.ReadyFuture;

/**
 * Provides a class for managing directories in FoundationDB.
 *
 * <p>
 *     The FoundationDB API provides directories as a tool for managing related
 *     {@link Subspace}s. Directories are a recommended approach for administering
 *     applications. Each application should create or open at least one directory
 *     to manage its subspaces.
 *
 *     For general guidance on directory usage, see the discussion in the
 *     <a href="/documentation/developer-guide.html#developer-guide-directories" target="_blank">Developer Guide</a>.
 * </p>
 * <p>
 *     Directories are identified by hierarchical paths analogous to the paths
 *     in a Unix-like file system. A path is represented as a List of strings.
 *     Each directory has an associated subspace used to store its content. The
 *     layer maps each path to a short prefix used for the corresponding
 *     subspace. In effect, directories provide a level of indirection for
 *     access to subspaces.
 * </p>
 */
public class DirectoryLayer implements Directory
{
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	private static final byte[] LITTLE_ENDIAN_LONG_ONE = { 1, 0, 0, 0, 0, 0, 0, 0 };
	private static final byte[] HIGH_CONTENTION_KEY = "hca".getBytes(UTF_8);
	private static final byte[] LAYER_KEY = "layer".getBytes(UTF_8);
	private static final byte[] VERSION_KEY = "version".getBytes(UTF_8);
	private static final long SUB_DIR_KEY = 0;
	private static final Integer[] VERSION = { 1, 0, 0 };

	static final byte[] EMPTY_BYTES = new byte[0];
	static final List<String> EMPTY_PATH = Collections.emptyList();
	static final byte[] DEFAULT_NODE_SUBSPACE_PREFIX =	{ (byte)0xFE };

	/**
	 * The default node {@link Subspace} used by a {@code DirectoryLayer} when none is specified.
	 */
	public static final Subspace DEFAULT_NODE_SUBSPACE = new Subspace(DEFAULT_NODE_SUBSPACE_PREFIX);

	/**
	 * The default content {@link Subspace} used by a {@code DirectoryLayer} when none is specified.
	 */
	public static final Subspace DEFAULT_CONTENT_SUBSPACE = new Subspace();

	private final Subspace rootNode;
	private final Subspace nodeSubspace;
	private final Subspace contentSubspace;
	private final HighContentionAllocator allocator;
	private final boolean allowManualPrefixes;

	private List<String> path = EMPTY_PATH;
	/**
	 * The layer string to pass to {@link Directory#createOrOpen(TransactionContext, List, byte[])} or
	 * {@link Directory#create(TransactionContext, List, byte[])} to create a {@code DirectoryPartition}.
	 */
	public static final byte[] PARTITION_LAYER = "partition".getBytes(Charset.forName("UTF-8"));

	private static DirectoryLayer defaultDirectoryLayer = new DirectoryLayer();

	/**
	 * Constructor for a {@code DirectoryLayer} formed with default node and
	 * content subspaces. A {@code DirectoryLayer}
	 * defines a new root directory. The node subspace and content subspace
	 * control where the directory metadata and contents, respectively, are
	 * stored. The default root directory has a node subspace with raw prefix
	 * {@code \xFE} and a content subspace with no prefix.
	 * Prefixes can not be specified in calls to {@link Directory#create(TransactionContext, List, byte[], byte[])}.
	 *
	 * @see #getDefault
	 */
	public DirectoryLayer() {
		this(DEFAULT_NODE_SUBSPACE, DEFAULT_CONTENT_SUBSPACE, false);
	}

	/**
	 * Constructor for a {@code DirectoryLayer} formed with default node and
	 * content subspaces. A {@code DirectoryLayer}
	 * defines a new root directory. The node subspace and content subspace
	 * control where the directory metadata and contents, respectively, are
	 * stored. The default root directory has a node subspace with raw prefix
	 * {@code \xFE} and a content subspace with no prefix.
	 *
	 * @param allowManualPrefixes whether or not prefixes can be specified in calls to
	 * {@link Directory#create(TransactionContext, List, byte[], byte[])}
	 */
	public DirectoryLayer(boolean allowManualPrefixes) {
		this(DEFAULT_NODE_SUBSPACE, DEFAULT_CONTENT_SUBSPACE, allowManualPrefixes);
	}

	/**
	 * Constructor for a {@code DirectoryLayer} formed with a specified node
	 * subspace and specified content subspace. A {@code DirectoryLayer}
	 * defines a new root directory. The node subspace and content subspace
	 * control where the directory metadata and contents, respectively, are
	 * stored. The default root directory has a node subspace with raw prefix
	 * {@code \xFE} and a content subspace with no prefix. Specifying more
	 * restrictive values for the node subspace and content subspace will allow
	 * using the directory layer alongside other content in a database.
	 * Prefixes can not be specified in calls to {@link Directory#create(TransactionContext, List, byte[], byte[])}.
	 *
	 * @param nodeSubspace a {@link Subspace} used to store directory metadata
	 * @param contentSubspace a {@link Subspace} used to store directory content
	 */
	public DirectoryLayer(Subspace nodeSubspace, Subspace contentSubspace) {
		this(nodeSubspace, contentSubspace, false);
	}

	/**
	 * Constructor for a {@code DirectoryLayer} formed with a specified node
	 * subspace and specified content subspace.  A {@code DirectoryLayer}
	 * defines a new root directory. The node subspace and content subspace
	 * control where the directory metadata and contents, respectively, are
	 * stored. The default root directory has a node subspace with raw prefix
	 * {@code \xFE} and a content subspace with no prefix. Specifying more
	 * restrictive values for the node subspace and content subspace will allow
	 * using the directory layer alongside other content in a database.
	 *
	 * @param nodeSubspace a {@link Subspace} used to store directory metadata
	 * @param contentSubspace a {@link Subspace} used to store directory content
	 * @param allowManualPrefixes whether or not prefixes can be specified in calls to
	 * {@link Directory#create(TransactionContext, List, byte[], byte[])}
	 */
	public DirectoryLayer(Subspace nodeSubspace, Subspace contentSubspace, boolean allowManualPrefixes) {
		this.nodeSubspace = nodeSubspace;
		this.contentSubspace = contentSubspace;
		// The root node is the one whose contents are the node subspace
		this.rootNode = nodeSubspace.get(nodeSubspace.getKey());
		this.allocator = new HighContentionAllocator(rootNode.get(HIGH_CONTENTION_KEY));
		this.allowManualPrefixes = allowManualPrefixes;
	}

	/**
	 * Creates a new {@code DirectoryLayer} formed with a specified node subspace and default content subspace.
	 * Prefixes can not be specified in calls to {@link Directory#create(TransactionContext, List, byte[], byte[])}.
	 *
	 * @param node_subspace a {@link Subspace} used to store directory metadata
	 * @return a {@code DirectoryLayer} formed with {@code node_subspace} and a default content subspace
	 */
	public static Directory createWithNodeSubspace(Subspace node_subspace) {
		return new DirectoryLayer(node_subspace, DEFAULT_CONTENT_SUBSPACE);
	}

	/**
	 * Creates a new {@code DirectoryLayer} formed with a default node subspace and specified content subspace.
	 * Prefixes can not be specified in calls to {@link Directory#create(TransactionContext, List, byte[], byte[])}.
	 *
	 * @param content_subspace a {@link Subspace} used to store directory content
	 * @return a {@code DirectoryLayer} formed with a {@code content_subspace} and a default node subspace
	 */
	public static Directory createWithContentSubspace(Subspace content_subspace) {
		return new DirectoryLayer(DEFAULT_NODE_SUBSPACE, content_subspace);
	}

	/**
	 * Gets the default instance of the DirectoryLayer. The default instance
	 * is created with the default node and content subspaces.
	 *
	 * Prefixes can not be specified in calls to {@link Directory#create(TransactionContext, List, byte[], byte[])}.
	 *
	 * @return the default {@code DirectoryLayer}
	 */
	public static DirectoryLayer getDefault() {
		return defaultDirectoryLayer;
	}

	/**
	 * Tests whether this {@code DirectoryLayer} is equal to {@code rhs}.
	 * Two {@code DirectoryLayer}s are equal if they have the same node subspace,
	 * content subspace, and path.
	 *
	 * @param rhs the object to check for equality
	 * @return {@code true} if this {@code DirectoryLayer} and {@code rhs} are equal
	 */
	@Override
	public boolean equals(Object rhs) {
		if(this == rhs) {
			return true;
		}
		if(rhs == null || getClass() != rhs.getClass()) {
			return false;
		}
		DirectoryLayer other = (DirectoryLayer)rhs;

		return (path == other.path || path.equals(other.path))
			&& nodeSubspace.equals(other.nodeSubspace)
			&& contentSubspace.equals(other.contentSubspace);
	}

	/**
	 * Produces a hash of this {@code DirectoryLayer} based on its path and subspaces.
	 * This satisfies the necessary requirements to allow this class to be used as keys
	 * in hash tables or as values in hash-based sets.
	 *
	 * @return a hash based on the path and subspaces of this {@code DirectoryLayer}
	 */
	@Override
	public int hashCode() {
		return path.hashCode() ^ (nodeSubspace.hashCode() * 179) ^ (contentSubspace.hashCode() * 937);
	}

	/**
	 * Sets path of directory to {@code path}
	 *
	 * @param path a {@code List<String>} specifying a path
	 */
	void setPath(List<String> path) {
		this.path = path;
	}

	/**
	 * Gets the path for the root node of this {@code DirectoryLayer}. Normally constructed
	 * {@code DirectoryLayer}s have an empty path, but {@code DirectoryLayer}s returned by
	 * {@link Directory#getDirectoryLayer} for {@link Directory}s inside of a partition
	 * could have non-empty paths.
	 *
	 * @return the path for the root node of this {@code DirectoryLayer}
	 */
	@Override
	public List<String> getPath() {
		return Collections.unmodifiableList(path);
	}

	/**
	 * Returns the layer byte string for this {@code DirectoryLayer}, which is always an empty
	 * array.
	 *
	 * @return an empty byte array
	 */
	@Override
	public byte[] getLayer() {
		return EMPTY_BYTES;
	}

	/**
	 * Returns {@code this}.
	 *
	 * @return {@code this}
	 */
	@Override
	public DirectoryLayer getDirectoryLayer() {
		return this;
	}

	/**
	 * Creates or opens the directory located at {@code path} (creating parent directories, if necessary).
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @return a {@link Future} which will be set to the created or opened {@link DirectorySubspace}
	 */
	@Override
	public Future<DirectorySubspace> createOrOpen(TransactionContext tcx, List<String> path) {
		return createOrOpen(tcx, path, EMPTY_BYTES);
	}

	/**
	 * Creates or opens the directory located at {@code path}(creating parent directories, if necessary).
	 * If the directory is new, then the {@code layer} byte string will be recorded as its layer.
	 * If the directory already exists, the {@code layer} byte string will be compared against the {@code layer}
	 * set when the directory was created.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link MismatchedLayerException} - if the directory has already been created with a different {@code layer} byte string</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @param layer a {@code byte[]} specifying a layer to set on a new directory or check for on an existing directory
	 * @return a {@link Future} which will be set to the created or opened {@link DirectorySubspace}
	 */
	@Override
	public Future<DirectorySubspace> createOrOpen(TransactionContext tcx, final List<String> path, final byte[] layer) {
		return tcx.runAsync(new Function<Transaction, Future<DirectorySubspace>>() {
			@Override
			public Future<DirectorySubspace> apply(Transaction tr) {
				return createOrOpenInternal(tr, tr, path, layer, null, true, true);
			}
		});
	}

	/**
	 * Opens the directory located at {@code path}.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if the directory does not exist</li>
	 * </ul>
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @return a {@link Future} which will be set to the opened {@link DirectorySubspace}
	 */
	@Override
	public Future<DirectorySubspace> open(ReadTransactionContext tcx, List<String> path) {
		return open(tcx, path, EMPTY_BYTES);
	}

	/**
	 * Opens the directory located at {@code path}.
	 * The {@code layer} byte string will be compared against the {@code layer} set when
	 * the directory was created.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link MismatchedLayerException} - if the directory was created with a different {@code layer} byte string</li>
	 *   <li>{@link NoSuchDirectoryException} - if the directory does not exist</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @param layer a {@code byte[]} specifying the expected layer
	 * @return a {@link Future} which will be set to the opened {@link DirectorySubspace}
	 */
	@Override
	public Future<DirectorySubspace> open(ReadTransactionContext tcx, final List<String> path, final byte[] layer) {
		return tcx.readAsync(new Function<ReadTransaction, Future<DirectorySubspace>>() {
			@Override
			public Future<DirectorySubspace> apply(ReadTransaction rtr) {
				return createOrOpenInternal(rtr, null, path, layer, null, false, true);
			}
		});
	}

	/**
	 * Creates a directory located at {@code path} (creating parent directories if necessary).
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryAlreadyExistsException} - if the given directory already exists</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @return a {@link Future} which will be set to the created {@link DirectorySubspace}
	 */
	@Override
	public Future<DirectorySubspace> create(TransactionContext tcx, List<String> path) {
		return create(tcx, path, EMPTY_BYTES, null);
	}

	/**
	 * Creates a directory located at {@code path} (creating parent directories if necessary).
	 * The {@code layer} byte string will be recorded as the new directory's layer and checked by
	 * future calls to {@link #open(ReadTransactionContext, List, byte[])}.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryAlreadyExistsException} - if the given directory already exists</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path of this {@code Directory}
	 * @param layer a {@code byte[]} specifying a layer to set for the directory
	 * @return a {@link Future} which will be set to the created {@link DirectorySubspace}
	 */
	@Override
	public Future<DirectorySubspace> create(TransactionContext tcx, List<String> path, byte[] layer) {
		return create(tcx, path, layer, null);
	}

	/**
	 * Creates a directory located at {@code path} (creating parent directories if necessary).
	 * The {@code layer} byte string will be recorded as the new directory's layer and checked by
	 * future calls to {@link #open(ReadTransactionContext, List, byte[])}. The specified {@code prefix}
	 * will be used for this directory's contents instead of allocating a prefix automatically.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryAlreadyExistsException} - if the given directory already exists</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path of this {@code Directory}
	 * @param layer a {@code byte[]} specifying a layer to set for the directory
	 * @param prefix a {@code byte[]} specifying the key prefix to use for the directory's contents
	 * @return a {@link Future} which will be set to the created {@link DirectorySubspace}
	 */
	@Override
	public Future<DirectorySubspace> create(TransactionContext tcx, final List<String> path, final byte[] layer, final byte[] prefix) {
		return tcx.runAsync(new Function<Transaction, Future<DirectorySubspace>>() {
			@Override
			public Future<DirectorySubspace> apply(Transaction tr) {
				return createOrOpenInternal(tr, tr, path, layer, prefix, true, false);
			}
		});
	}

	/**
	 * This method should not be called on a {@code DirectoryLayer}. Calling this method will result in the returned
	 * {@link Future} being set to a {@link DirectoryMoveException}.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryMoveException}</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param newAbsolutePath a {@code List<String>} specifying a path
	 * @return a {@link Future} which will be set to a {@link DirectoryMoveException}
	 */
	@Override
	public Future<DirectorySubspace> moveTo(TransactionContext tcx, List<String> newAbsolutePath) {
		return new ReadyFuture<DirectorySubspace>(new DirectoryMoveException("The root directory cannot be moved.", path, newAbsolutePath));
	}

	/**
	 * Moves the directory located at {@code oldPath} to {@code newPath}.
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
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if no {@code Directory} exists at {@code oldPath}</li>
	 *   <li>{@link DirectoryAlreadyExistsException} - if a directory already exists at {@code newPath}</li>
	 *   <li>{@link DirectoryMoveException} - if an invalid move location is specified</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param oldPath a {@code List<String>} specifying the path of the directory to move
	 * @param newPath a {@code List<String>} specifying the path to move to
	 * @return a {@link Future} which will be set to the {@link DirectorySubspace} for this {@code Directory}
	 * at its new location.
	 */
	@Override
	public Future<DirectorySubspace> move(final TransactionContext tcx, final List<String> oldPath, final List<String> newPath) {
		final List<String> oldPathCopy = new ArrayList<String>(oldPath);
		final List<String> newPathCopy = new ArrayList<String>(newPath);

		return tcx.runAsync(new Function<Transaction, Future<DirectorySubspace>>() {
			@Override
			public Future<DirectorySubspace> apply(final Transaction tr) {
				return checkOrWriteVersion(tr)
				.flatMap(new Function<Void, Future<List<Node>>>() {
					@Override
					public Future<List<Node>> apply(Void ignore) {
						if(oldPathCopy.size() <= newPathCopy.size() && oldPathCopy.equals(newPathCopy.subList(0, oldPathCopy.size())))
							throw new DirectoryMoveException("The destination directory cannot be a subdirectory of the source directory.", toAbsolutePath(oldPathCopy), toAbsolutePath(newPathCopy));

						ArrayList<Future<Node>> futures = new ArrayList<Future<Node>>();
						futures.add(new NodeFinder(oldPathCopy).find(tr).flatMap(new NodeMetadataLoader(tr)));
						futures.add(new NodeFinder(newPathCopy).find(tr).flatMap(new NodeMetadataLoader(tr)));

						return AsyncUtil.getAll(futures);
					}
				})
				.flatMap(new Function<List<Node>, Future<DirectorySubspace>>() {
					@Override
					public Future<DirectorySubspace> apply(List<Node> nodes) {
						final Node oldNode = nodes.get(0);
						final Node newNode = nodes.get(1);

						if(!oldNode.exists())
							throw new NoSuchDirectoryException(toAbsolutePath(oldPathCopy));

						if(oldNode.isInPartition(false) || newNode.isInPartition(false)) {
							if(!oldNode.isInPartition(false) || !newNode.isInPartition(false) || !oldNode.path.equals(newNode.path))
								throw new DirectoryMoveException("Cannot move between partitions.", toAbsolutePath(oldPathCopy), toAbsolutePath(newPathCopy));

							return newNode.getContents().move(tr, oldNode.getPartitionSubpath(), newNode.getPartitionSubpath());
						}

						if(newNode.exists())
							throw new DirectoryAlreadyExistsException(toAbsolutePath(newPathCopy));

						final List<String> parentPath = PathUtil.popBack(newPathCopy);
						return new NodeFinder(parentPath).find(tr)
						.flatMap(new Function<Node, Future<DirectorySubspace>>() {
							@Override
							public Future<DirectorySubspace> apply(Node parentNode) {
								if(!parentNode.exists())
									throw new NoSuchDirectoryException(toAbsolutePath(parentPath));

								tr.set(
									parentNode.subspace.get(SUB_DIR_KEY).get(getLast(newPathCopy)).getKey(),
									contentsOfNode(oldNode.subspace, EMPTY_PATH, EMPTY_BYTES).getKey()
								);

								return removeFromParent(tr, oldPathCopy)
								.map(new Function<Void, DirectorySubspace>() {
									@Override
									public DirectorySubspace apply(Void ignore) {
										return contentsOfNode(oldNode.subspace, newPathCopy, oldNode.layer);
									}
								});
							}
						});
					}
				});
			}
		});
	}

	/**
	 * This method should not be called on the root directory. Calling this method will result in the returned
	 * {@link Future} being set to a {@link DirectoryException}.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryException}</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @return a {@link Future} which will be set to a {@link DirectoryException}
	 */
	@Override
	public Future<Void> remove(TransactionContext tcx) {
		return remove(tcx, EMPTY_PATH);
	}

	/**
	 * Removes the directory located at {@code path} and all of its subdirectories,
	 * as well as all of their contents.
	 *
	 * <p><i>
	 *   Warning: Clients that have already opened the directory might
	 *   still insert data into its contents after it is removed.
	 * </i></p>
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if no directory exists at {@code path}</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @return a {@link Future} which will be set once the {@code Directory} has been removed
	 */
	@Override
	public Future<Void> remove(TransactionContext tcx, List<String> path) {
		return AsyncUtil.success(removeInternal(tcx, path, true));
	}

	/**
     * This method should not be called on the root directory. Calling this method will result in the returned
	 * {@link Future} being set to a {@link DirectoryException}.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link DirectoryException}</li>
	 * </ul>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @return a {@link Future} which will be set to a {@link DirectoryException}
	 */
	@Override
	public Future<Boolean> removeIfExists(TransactionContext tcx) {
		return removeIfExists(tcx, EMPTY_PATH);
	}

	/**
	 * Removes the directory located at {@code subpath} and all of its subdirectories,
	 * as well as all of their contents.
	 *
	 * <p><i>
	 *   Warning: Clients that have already opened the directory might
	 *   still insert data into its contents after it is removed.
	 * </i></p>
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @return a {@link Future} which will be set to true once the {@code Directory} has been removed,
	 * or false if it didn't exist.
	 */
	@Override
	public Future<Boolean> removeIfExists(TransactionContext tcx, List<String> path) {
		return removeInternal(tcx, path, false);
	}

	/**
	 * List the subdirectories of the root directory.
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @return a {@link Future} which will be set to a {@code List<String>} of names of the subdirectories
	 * of the root directory. Each name is a unicode string representing the last component of a
	 * subdirectory's path.
	 */
	@Override
	public Future<List<String>> list(ReadTransactionContext tcx) {
		return list(tcx, EMPTY_PATH);
	}

	/**
	 * List the subdirectories of the directory at a given {@code path}.
	 *
	 * <p>The returned {@link Future} can be set to the following errors:</p>
	 * <ul>
	 *   <li>{@link NoSuchDirectoryException} - if no directory exists at {@code path}</li>
	 * </ul>
	 *
	 * @param tcx the {@link ReadTransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path
	 * @return a {@link Future} which will be set to a {@code List<String>} of names of the subdirectories
	 * of the directory at {@code path}. Each name is a unicode string representing the last component
	 * of a subdirectory's path.
	 */
	@Override
	public Future<List<String>> list(final ReadTransactionContext tcx, final List<String> path) {
		final List<String> pathCopy = new ArrayList<String>(path);

		return tcx.readAsync(new Function<ReadTransaction, Future<List<String>>>() {
			@Override
			public Future<List<String>> apply(final ReadTransaction tr) {
				return checkVersion(tr)
				.flatMap(new Function<Void, Future<Node>>() {
					@Override
					public Future<Node> apply(Void ignore) {
						return new NodeFinder(pathCopy).find(tr).flatMap(new NodeMetadataLoader(tr));
					}
				})
				.flatMap(new Function<Node, Future<List<String>>>() {
					@Override
					public Future<List<String>> apply(Node node) {
						if(!node.exists())
							throw new NoSuchDirectoryException(toAbsolutePath(pathCopy));

						if(node.isInPartition(true))
							return node.getContents().list(tr, node.getPartitionSubpath());

						final Subspace subdir = node.subspace.get(SUB_DIR_KEY);

						return AsyncUtil.collect(
								AsyncUtil.mapIterable(tr.getRange(subdir.range()),
									new Function<KeyValue, String>() {
										@Override
										public String apply(KeyValue o) {
											return subdir.unpack(o.getKey()).getString(0);
										}
									}));
					}
				});
			}
		});
	}

	/**
	 * Returns {@code true}.
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @return {@code true}
	 */
	@Override
	public Future<Boolean> exists(ReadTransactionContext tcx) {
		return new ReadyFuture<Boolean>(true);
	}

	/**
	 * Checks if the directory located at {@code path} exists.
	 *
	 * @param tcx the {@link TransactionContext} to execute this operation in
	 * @param path a {@code List<String>} specifying a path of this {@code Directory}
	 * @return a {@link Future} which will be set to {@code true} if the specified directory exists, or {@code false} if it
	 * doesn't
	 */
	@Override
	public Future<Boolean> exists(final ReadTransactionContext tcx, final List<String> path) {
		final List<String> pathCopy = new ArrayList<String>(path);

		return tcx.readAsync(new Function<ReadTransaction, Future<Boolean>>() {
			@Override
			public Future<Boolean> apply(final ReadTransaction tr) {
				return checkVersion(tr)
				.flatMap(new Function<Void, Future<Node>>() {
					@Override
					public Future<Node> apply(Void ignore) {
						return new NodeFinder(pathCopy).find(tr).flatMap(new NodeMetadataLoader(tr));
					};
				})
				.flatMap(new Function<Node, Future<Boolean>>() {
					@Override
					public Future<Boolean> apply(Node node) {
						if(!node.exists())
							return new ReadyFuture<Boolean>(false);
						else if(node.isInPartition(false))
							return node.getContents().exists(tr, node.getPartitionSubpath());

						return new ReadyFuture<Boolean>(true);
					}
				});
			}
		});
	}

	//
	// Internal
	//

	private Subspace nodeWithPrefix(byte[] prefix) {
		if(prefix == null) {
			return null;
		}
		return nodeSubspace.get(prefix);
	}

	private Future<Subspace> nodeContainingKey(final ReadTransaction tr, final byte[] key) {
		// Right now this is only used for _is_prefix_free(), but if we add
		// parent pointers to directory nodes, it could also be used to find a
		// path based on a key.
		if(ByteArrayUtil.startsWith(key, nodeSubspace.getKey())) {
			return new ReadyFuture<Subspace>(rootNode);
		}

		return tr.getRange(nodeSubspace.range().begin, ByteArrayUtil.join(nodeSubspace.pack(key), new byte[]{0x00}), 1, true)
		.asList()
		.map(new Function<List<KeyValue>, Subspace>() {
			@Override
			public Subspace apply(List<KeyValue> results) {
				if(results.size() > 0) {
					byte[] resultKey = results.get(0).getKey();
					byte[] prevPrefix = nodeSubspace.unpack(resultKey).getBytes(0);
					if(ByteArrayUtil.startsWith(key, prevPrefix)) {
						return nodeWithPrefix(prevPrefix);
					}
				}

				return null;
			}
		});
	}

	private List<String> toAbsolutePath(List<String> subPath) {
		return PathUtil.join(path, subPath);
	}

	private DirectorySubspace contentsOfNode(Subspace node, List<String> path, byte[] layer) {
		byte[] prefix = nodeSubspace.unpack(node.getKey()).getBytes(0);

		if(Arrays.equals(layer, DirectoryLayer.PARTITION_LAYER))
			return new DirectoryPartition(toAbsolutePath(path), prefix, this);
		else
			return new DirectorySubspace(toAbsolutePath(path), prefix, this, layer);
	}

	private Future<Boolean> removeInternal(final TransactionContext tcx, final List<String> path, final boolean mustExist) {
		final List<String> pathCopy = new ArrayList<String>(path);

		return tcx.runAsync(new Function<Transaction, Future<Boolean>>() {
			@Override
			public Future<Boolean> apply(final Transaction tr) {
				return checkOrWriteVersion(tr)
				.flatMap(new Function<Void, Future<Node>>() {
					@Override
					public Future<Node> apply(Void ignore) {
						if(pathCopy.size() == 0)
							throw new DirectoryException("The root directory cannot be removed.", toAbsolutePath(pathCopy));

						return new NodeFinder(pathCopy).find(tr).flatMap(new NodeMetadataLoader(tr));
					}
				})
				.flatMap(new Function<Node, Future<Boolean>>() {
					@Override
					public Future<Boolean> apply(Node node) {
						if(!node.exists()) {
							if(mustExist)
								throw new NoSuchDirectoryException(toAbsolutePath(pathCopy));
							else
								return new ReadyFuture<Boolean>(false);
						}

						if(node.isInPartition(false))
							return node.getContents().getDirectoryLayer().removeInternal(tr, node.getPartitionSubpath(), mustExist);
						else {
							ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>();
							futures.add(removeRecursive(tr, node.subspace));
							futures.add(removeFromParent(tr, pathCopy));

							return AsyncUtil.tag(AsyncUtil.whenAll(futures), true);
						}
					}
				});
			}
		});
	}

	private Future<Void> removeFromParent(final Transaction tr, final List<String> path) {
		return new NodeFinder(PathUtil.popBack(path)).find(tr)
		.map(new Function<Node, Void>() {
			@Override
			public Void apply(Node parent) {
				tr.clear(parent.subspace.get(SUB_DIR_KEY).get(getLast(path)).getKey());
				return null;
			}
		});
	}

	private Future<Void> removeRecursive(final Transaction tr, final Subspace node) {
		Subspace subdir = node.get(SUB_DIR_KEY);
		final AsyncIterator<KeyValue> rangeItr = tr.getRange(subdir.range()).iterator();

		tr.clear(Range.startsWith(nodeSubspace.unpack(node.getKey()).getBytes(0)));
		tr.clear(node.range());

		Future<Void> result = AsyncUtil.whileTrue(new Function<Void, Future<Boolean>>() {
			@Override
			public Future<Boolean> apply(Void ignore) {
				Future<Void> subdirRemoveFuture;
				if(rangeItr.onHasNext().isDone() && rangeItr.hasNext())
					subdirRemoveFuture = removeRecursive(tr, nodeWithPrefix(rangeItr.next().getValue()));
				else
					subdirRemoveFuture = ReadyFuture.DONE;

				return subdirRemoveFuture
				.flatMap(new Function<Void, Future<Boolean>>() {
					@Override
					public Future<Boolean> apply(Void ignore) {
						return rangeItr.onHasNext();
					}
				});
			}
		});

		result.onReady(new Runnable() {
			@Override
			public void run() {
				rangeItr.dispose();
			}
		});

		return result;
	}

	private Future<Boolean> isPrefixFree(final ReadTransaction tr, final byte[] prefix) {
		// Returns true if the given prefix does not "intersect" any currently
		// allocated prefix (including the root node). This means that it neither
		// contains any other prefix nor is contained by any other prefix.
		if(prefix == null || prefix.length == 0)
			return new ReadyFuture<Boolean>(false);

		return nodeContainingKey(tr, prefix).
		flatMap(new Function<Subspace, Future<Boolean>>() {
			@Override
			public Future<Boolean> apply(Subspace node) {
				if(node != null)
					return new ReadyFuture<Boolean>(false);

				final AsyncIterator<KeyValue> it = tr.getRange(nodeSubspace.pack(prefix), nodeSubspace.pack(ByteArrayUtil.strinc(prefix)), 1).iterator();
				Future<Boolean> result = it.onHasNext()
				.map(new Function<Boolean, Boolean>() {
					@Override
					public Boolean apply(Boolean hasNext) {
						return !hasNext;
					}
				});

				result.onReady(new Runnable() {
					@Override
					public void run() {
						it.dispose();
					}
				});

				return result;
			}
		});
	}

	private Future<byte[]> getVersionValue(final ReadTransaction tr) {
		return tr.get(rootNode.pack(VERSION_KEY));
	}

	private Future<Void> checkOrWriteVersion(final Transaction tr) {
		return getVersionValue(tr).map(new WritableVersionCheck(tr));
	}

	private Future<Void> checkVersion(final ReadTransaction tr) {
		return getVersionValue(tr).map(new VersionCheck());
	}

	private Future<DirectorySubspace> createOrOpenInternal(final ReadTransaction rtr,
																   final Transaction tr,
																   final List<String> path,
																   final byte[] layer,
																   final byte[] prefix,
																   final boolean allowCreate,
																   final boolean allowOpen)
	{
		final List<String> pathCopy = new ArrayList<String>(path);

		if(prefix != null && !allowManualPrefixes) {
			String errorMessage;
			if(this.path.size() == 0)
				errorMessage = "Cannot specify a prefix unless manual prefixes are enabled.";
			else
				errorMessage = "Cannot specify a prefix in a partition.";

			return new ReadyFuture<DirectorySubspace>(new IllegalArgumentException(errorMessage));
		}

		return checkVersion(rtr)
			.flatMap(new Function<Void, Future<Node>>() {
				@Override
				public Future<Node> apply(Void ignore) {
					// Root directory contains node metadata and so may not be opened.
					if(pathCopy.size() == 0) {
						throw new IllegalArgumentException("The root directory may not be opened.");
					}

					return new NodeFinder(pathCopy).find(rtr).flatMap(new NodeMetadataLoader(rtr));
				}
			})
			.flatMap(new Function<Node, Future<DirectorySubspace>>() {
				@Override
				public Future<DirectorySubspace> apply(final Node existingNode) {
					if(existingNode.exists()) {
						if(existingNode.isInPartition(false)) {
							List<String> subpath = existingNode.getPartitionSubpath();
							DirectoryLayer directoryLayer = existingNode.getContents().getDirectoryLayer();
							return directoryLayer.createOrOpenInternal(
									rtr, tr, subpath, layer, prefix, allowCreate, allowOpen);
						}

						DirectorySubspace opened = openInternal(pathCopy, layer, existingNode, allowOpen);
						return new ReadyFuture<DirectorySubspace>(opened);
					}
					else
						return createInternal(tr, pathCopy, layer, prefix, allowCreate);
				}
			});
	}

	private DirectorySubspace openInternal(final List<String> path,
															final byte[] layer,
															final Node existingNode,
															final boolean allowOpen)
	{
		if(!allowOpen) {
			throw new DirectoryAlreadyExistsException(toAbsolutePath(path));
		}
		else {
			if(layer.length > 0 && !Arrays.equals(layer, existingNode.layer)) {
				throw new MismatchedLayerException(toAbsolutePath(path), existingNode.layer, layer);
			}

			return existingNode.getContents();
		}
	}

	private Future<DirectorySubspace> createInternal(final Transaction tr,
															final List<String> path,
															final byte[] layer,
															final byte[] prefix,
															final boolean allowCreate)
	{
		if(!allowCreate) {
			throw new NoSuchDirectoryException(toAbsolutePath(path));
		}

		return checkOrWriteVersion(tr)
		.flatMap(new Function<Void, Future<byte[]>>() {
			@Override
			public Future<byte[]> apply(Void ignore) {
				if(prefix == null) {
					return allocator.allocate(tr)
					.flatMap(new Function<byte[], Future<byte[]>>() {
						@Override
						public Future<byte[]> apply(byte[] allocated) {
							final byte[] finalPrefix = ByteArrayUtil.join(contentSubspace.getKey(), allocated);
							return tr.getRange(Range.startsWith(finalPrefix), 1)
							.asList()
							.map(new Function<List<KeyValue>, byte[]>() {
								@Override
								public byte[] apply(List<KeyValue> results) {
									if(results.size() > 0) {
										throw new IllegalStateException("The database has keys stored at the prefix chosen by the automatic " +
																		"prefix allocator: " + ByteArrayUtil.printable(finalPrefix) + ".");
									}

									return finalPrefix;
								}
							});
						}
					});
				}
				else
					return new ReadyFuture<byte[]>(prefix);
			}
		})
		.flatMap(new Function<byte[], Future<DirectorySubspace>>() {
			@Override
			public Future<DirectorySubspace> apply(final byte[] actualPrefix) {
				return isPrefixFree(prefix == null ? tr.snapshot() : tr, actualPrefix)
				.flatMap(new Function<Boolean, Future<Subspace>>() {
					@Override
					public Future<Subspace> apply(Boolean prefixFree) {
						if(!prefixFree) {
							if(prefix == null) {
								throw new IllegalStateException("The directory layer has manually allocated prefixes that conflict " +
																"with the automatic prefix allocator.");
							}
							else
								throw new IllegalArgumentException("Prefix already in use: " + ByteArrayUtil.printable(actualPrefix) + ".");
						}
						else if(path.size() > 1) {
							return createOrOpen(tr, PathUtil.popBack(path))
							.map(new Function<DirectorySubspace, Subspace>() {
								@Override
								public Subspace apply(DirectorySubspace dir) {
									return nodeWithPrefix(dir.getKey());
								}
							});
						}
						else
							return new ReadyFuture<Subspace>(rootNode);
					}
				})
				.map(new Function<Subspace, DirectorySubspace>() {
					@Override
					public DirectorySubspace apply(Subspace parentNode) {
						if(parentNode == null)
							throw new IllegalStateException("The parent directory does not exist."); //Shouldn't happen
						Subspace node = nodeWithPrefix(actualPrefix);
						tr.set(parentNode.get(SUB_DIR_KEY).get(getLast(path)).getKey(), actualPrefix);
						tr.set(node.get(LAYER_KEY).getKey(), layer);
						return contentsOfNode(node, path, layer);
					}
				});
			}
		});
	}

	//
	// Helpers
	//

	private static long unpackLittleEndian(byte[] bytes) {
		assert bytes.length == 8;
		int value = 0;
		for(int i = 0; i < 8; ++i) {
			value += (bytes[i] << (i * 8));
		}
		return value;
	}

	private static String getLast(List<String> list) {
		assert list.size() > 0;
		return list.get(list.size() - 1);
	}

	private class VersionCheck implements Function<byte[], Void> {
		@Override
		public Void apply(byte[] versionBytes) {
			if(versionBytes == null) {
				return null;
			}

			ByteBuffer versionBuf = ByteBuffer.wrap(versionBytes);
			versionBuf.order(ByteOrder.LITTLE_ENDIAN);

			Integer version[] = new Integer[3];
			for(int i = 0; i < version.length; ++i)
				version[i] = versionBuf.getInt();

			String dirVersion = String.format("version %d.%d.%d", (Object[])version);
			String layerVersion = String.format("directory layer %d.%d.%d", (Object[])VERSION);

			throwOnError(version, dirVersion, layerVersion);

			return null;
		}

		protected void throwOnError(Integer[] version, String dirVersion,
				String layerVersion) {
			if(version[0] > VERSION[0])
				throw new DirectoryVersionException("Cannot load directory with " + dirVersion + " using " + layerVersion + ".");
		}
	}

	private class WritableVersionCheck extends VersionCheck {
		private final Transaction tr;

		private WritableVersionCheck(Transaction tr) {
			this.tr = tr;
		}

		@Override
		public Void apply(byte[] versionBytes) {
			if(versionBytes == null) {
				// initializeVersion
				ByteBuffer buf = ByteBuffer.allocate(VERSION.length * 4);
				buf.order(ByteOrder.LITTLE_ENDIAN);
				for(int ver : VERSION)
					buf.putInt(ver);

				tr.set(rootNode.pack(VERSION_KEY), buf.array());
				return null;
			}

			return super.apply(versionBytes);
		}

		@Override
		protected void throwOnError(Integer[] version, String dirVersion,
				String layerVersion) {
			super.throwOnError(version, dirVersion, layerVersion);
			if(version[1] > VERSION[1])
				throw new DirectoryVersionException("Directory with " + dirVersion + " is read-only when opened with " + layerVersion + ".");
		}
	}

	private class NodeFinder {
		private List<String> path;
		private int index;
		private Node node;
		private List<String> currentPath;

		public NodeFinder(List<String> path) {
			this.path = path;
		}

		public Future<Node> find(final ReadTransaction tr) {
			index = 0;
			node = new Node(rootNode, currentPath, path);
			currentPath = new ArrayList<String>();

			return AsyncUtil.whileTrue(new Function<Void, Future<Boolean>>() {
				@Override
				public Future<Boolean> apply(Void ignore) {
					if(index == path.size())
						return new ReadyFuture<Boolean>(false);

					return tr.get(node.subspace.get(SUB_DIR_KEY).get(path.get(index)).getKey())
					.flatMap(new Function<byte[], Future<Boolean>>() {
						@Override
						public Future<Boolean> apply(byte[] key) {
							currentPath.add(path.get(index));
							node = new Node(nodeWithPrefix(key), currentPath, path);

							if(!node.exists())
								return new ReadyFuture<Boolean>(false);

							return node.loadMetadata(tr)
							.map(new Function<Node, Boolean>() {
								@Override
								public Boolean apply(Node ignore) {
									++index;
									return !Arrays.equals(node.layer, DirectoryLayer.PARTITION_LAYER);
								}
							});
						}
					});
				}
			})
			.map(new Function<Void, Node>() {
				@Override
				public Node apply(Void ignore) {
					return node;
				}
			});
		}
	}

	private static class NodeMetadataLoader implements Function<Node, Future<Node>> {
		private final ReadTransaction tr;

		public NodeMetadataLoader(ReadTransaction tr) {
			this.tr = tr;
		}

		@Override
		public Future<Node> apply(Node node) {
			return node.loadMetadata(tr);
		}
	}

	private class Node {
		public final Subspace subspace;
		public final List<String> path;
		public final List<String> targetPath;
		public byte[] layer;

		private boolean loadedMetadata;

		public Node(Subspace subspace, List<String> path, List<String> targetPath) {
			this.subspace = subspace;
			this.path = path;
			this.targetPath = targetPath;

			layer = null;
			loadedMetadata = false;
		}

		public boolean exists() {
			return subspace != null;
		}

		public Future<Node> loadMetadata(ReadTransaction tr) {
			if(!exists()) {
				loadedMetadata = true;
				return new ReadyFuture<Node>(this);
			}

			return tr.get(subspace.pack(new Tuple().add(LAYER_KEY)))
			.map(new Function<byte[], Node>() {
				@Override
				public Node apply(byte[] value) {
					layer = value;
					loadedMetadata = true;
					return Node.this;
				}
			});
		}

		public void ensureMetadataLoaded() {
			if(!loadedMetadata)
				throw new IllegalStateException("Metadata for node has not been loaded");
		}

		public boolean isInPartition(boolean includeEmptySubpath) {
			ensureMetadataLoaded();
			return exists() && Arrays.equals(layer, DirectoryLayer.PARTITION_LAYER) && (includeEmptySubpath || targetPath.size() > path.size());
		}

		public List<String> getPartitionSubpath() {
			ensureMetadataLoaded();
			return targetPath.subList(path.size(), targetPath.size());
		}

		public DirectorySubspace getContents() {
			ensureMetadataLoaded();
			return contentsOfNode(subspace, path, layer);
		}
	}

	private static class PrefixFinder {
		private final Random random;

		private long windowStart;
		private int windowSize;

		private long candidate;
		private boolean restart;

		public PrefixFinder() {
			this.random = new Random();
			this.windowStart = 0;
		}

		public Future<byte[]> find(final Transaction tr, final HighContentionAllocator allocator) {
			return AsyncUtil.whileTrue(new Function<Void, Future<Boolean>>() {
				@Override
				public Future<Boolean> apply(Void ignore) {
					final AsyncIterator<KeyValue> rangeItr = tr.snapshot().getRange(allocator.counters.range(), 1, true).iterator();
					Future<Boolean> result = rangeItr.onHasNext()
					.map(new Function<Boolean, Void>() {
						@Override
						public Void apply(Boolean hasNext) {
							if(hasNext) {
								KeyValue kv = rangeItr.next();
								windowStart = allocator.counters.unpack(kv.getKey()).getLong(0);
							}

							return null;
						}
					})
					.flatMap(new Function<Void, Future<Void>>() {
						@Override
						public Future<Void> apply(Void ignore) {
							return chooseWindow(tr, allocator);
						}
					})
					.flatMap(new Function<Void, Future<Boolean>>() {
						@Override
						public Future<Boolean> apply(Void ignore) {
							return choosePrefix(tr, allocator); // false exits the loop (i.e. we have a valid prefix)
						}
					});

					result.onReady(new Runnable() {
						@Override
						public void run() {
							rangeItr.dispose();
						}
					});

					return result;
				}
			})
			.map(new Function<Void, byte[]>() {
				@Override
				public byte[] apply(Void ignore) {
					return Tuple.from(candidate).pack();
				}
			});
		}

		public Future<Void> chooseWindow(final Transaction tr, final HighContentionAllocator allocator) {
			final long initialWindowStart = windowStart;
			return AsyncUtil.whileTrue(new Function<Void, Future<Boolean>>() {
				@Override
				public Future<Boolean> apply(Void ignore) {
					final byte[] counterKey = allocator.counters.get(windowStart).getKey();

					Range oldCounters = new Range(allocator.counters.getKey(), counterKey);
					Range oldAllocations = new Range(allocator.recent.getKey(), allocator.recent.get(windowStart).getKey());

					Future<byte[]> newCountRead;
					// SOMEDAY: synchronize on something transaction local
					synchronized(HighContentionAllocator.class) {
						if(windowStart > initialWindowStart) {
							tr.clear(oldCounters);
							tr.options().setNextWriteNoWriteConflictRange();
							tr.clear(oldAllocations);
						}

						tr.mutate(MutationType.ADD, counterKey, LITTLE_ENDIAN_LONG_ONE);
						newCountRead = tr.snapshot().get(counterKey);
					}

					return newCountRead
					.map(new Function<byte[], Boolean>() {
						@Override
						public Boolean apply(byte[] newCountBytes) {
							long newCount = newCountBytes == null ? 0 : unpackLittleEndian(newCountBytes);
							windowSize = getWindowSize(windowStart);
							if(newCount * 2 >= windowSize) {
								windowStart += windowSize;
								return true;
							}

							return false; // exit the loop
						}
					});
				}
			});
		}

		public Future<Boolean> choosePrefix(final Transaction tr, final HighContentionAllocator allocator) {
			restart = false;
			return AsyncUtil.whileTrue(new Function<Void, Future<Boolean>>() {
				@Override
				public Future<Boolean> apply(Void ignore) {
					// As of the snapshot being read from, the window is less than half
					// full, so this should be expected to take 2 tries.  Under high
					// contention (and when the window advances), there is an additional
					// subsequent risk of conflict for this transaction.
					candidate = windowStart + random.nextInt(windowSize);
					final byte[] allocationKey = allocator.recent.get(candidate).getKey();
					Range countersRange = allocator.counters.range();

					AsyncIterable<KeyValue> counterRange;
					Future<byte[]> allocationTemp;
					// SOMEDAY: synchronize on something transaction local
					synchronized(HighContentionAllocator.class) {
						counterRange = tr.snapshot().getRange(countersRange, 1, true);
						allocationTemp = tr.get(allocationKey);
						tr.options().setNextWriteNoWriteConflictRange();
						tr.set(allocationKey, EMPTY_BYTES);
					}

					final Future<List<KeyValue>> lastCounter = counterRange.asList();
					final Future<byte[]> allocation = allocationTemp;

					List<Future<Void>> futures = new ArrayList<Future<Void>>();
					futures.add(AsyncUtil.success(lastCounter));
					futures.add(AsyncUtil.success(allocation));

					return AsyncUtil.whenAll(futures)
					.map(new Function<Void, Boolean>() {
						@Override
						public Boolean apply(Void ignore) {
							long currentWindowStart = 0;
							if(!lastCounter.get().isEmpty()) {
								currentWindowStart = allocator.counters.unpack(lastCounter.get().get(0).getKey()).getLong(0);
							}

							if(currentWindowStart > windowStart) {
								restart = true;
								return false; // exit the loop and rerun the allocation from the beginning
							}

							if(allocation.get() == null) {
								tr.addWriteConflictKey(allocationKey);
								return false; // exit the loop and return this candidate
							}

							return true;
						}
					});
				}
			})
			.map(new Function<Void, Boolean>() {
				@Override
				public Boolean apply(Void ignore) {
					return restart;
				}
			});
		}

		private static int getWindowSize(long start) {
			// Larger window sizes are better for high contention, smaller sizes for
			// keeping the keys small. But if there are many allocations, the keys
			// can't be too small. So start small and scale up.  We don't want this
			// to ever get *too* big because we have to store about window_size/2
			// recent items.
			if(start < 255) {
				return 64;
			}
			if(start < 65535) {
				return 1024;
			}
			return 8192;
		}
	}

	private static class HighContentionAllocator {
		public final Subspace counters;
		public final Subspace recent;

		public HighContentionAllocator(Subspace subspace) {
			this.counters = subspace.get(0);
			this.recent = subspace.get(1);
		}

		/**
		 * Returns a byte string that:
		 * <ol>
		 *     <li>has never and will never be returned by another call to this method on the same subspace</li>
		 *     <li>is nearly as short as possible given the above</li>
		 * </ol>
		 */
		public Future<byte[]> allocate(final Transaction tr) {
			return new PrefixFinder().find(tr, this);
		}
	}
}
