/*
 * Transaction.java
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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.apple.foundationdb.tuple.Tuple;

/**
 * A Transaction represents a FoundationDB database transaction. All operations on FoundationDB
 *  take place, explicitly or implicitly, through a Transaction.<br>
 *  <br>
 * In FoundationDB, a transaction is a mutable snapshot of a database. All read and write operations
 *  on a transaction see and modify an otherwise-unchanging version of the database and only change
 *  the underlying database if and when the transaction is committed. Read operations do see the
 *  effects of previous write operations on the same transaction. Committing a transaction usually
 *  succeeds in the absence of
 *  <a href="/foundationdb/developer-guide.html#developer-guide-transaction-conflicts" target="_blank">conflicts</a>.<br>
 *  <br>
 * Transactions group operations into a unit with the properties of atomicity, isolation, and
 *  durability. Transactions also provide the ability to maintain an application's invariants or
 *  integrity constraints, supporting the property of consistency. Together these properties are
 *  known as <a href="/foundationdb/developer-guide.html#acid" target="_blank">ACID</a>.<br>
 *  <br>
 * Transactions are also causally consistent: once a transaction has been successfully committed,
 *  all subsequently created transactions will see the modifications made by it.
 * The most convenient way for a developer to manage the lifecycle and retrying
 *  of a {@code Transaction} is to use {@link Database#run(Function)}. Otherwise, the client
 *  must have retry logic for fatal failures, failures to commit, and other transient errors.<br>
 * <br>
 * Keys and values in FoundationDB are byte arrays. To encode other data types, see the
 *  {@link Tuple Tuple API} and
 *  <a href="/foundationdb/data-modeling.html#data-modeling-tuples" target="_blank">tuple layer documentation</a>.<br>
 * <br>
 * When used as a {@link TransactionContext}, the methods {@code run()} and
 *  {@code runAsync()} on a {@code Transaction} will simply attempt the operations
 *  without any retry loop.<br>
 * <br>
 * <b>Note:</b> Client must call {@link #commit()} and wait on the result on all transactions, even
 *  ones that only read. This is done automatically when using the retry loops from
 *  {@link Database#run(Function)}. This is because outstanding reads originating from a
 *  {@code Transaction} will be cancelled when a {@code Transaction} is garbage collected.
 *  Since the garbage collector reserves the right to collect an in-scope object if it
 *  determines that there are no subsequent references it it, this can happen in seemingly
 *  innocuous situations. {@code CompletableFuture}s returned from {@code commit()} will block until
 *  all reads are complete, thereby saving the calling code from this potentially confusing
 *  situation.<br>
 * <br>
 * <b>Note:</b> All keys with a first byte of {@code 0xff} are reserved for internal use.<br>
 * <br>
 * <b>Note:</b> Java transactions automatically set the {@link TransactionOptions#setUsedDuringCommitProtectionDisable}
 *  option. This is because the Java bindings disallow use of {@code Transaction} objects after {@link #onError}
 *  is called.<br>
 * <br>
 * <b>Note:</b> {@code Transaction} objects must be {@link #close closed} when no longer
 *  in use in order to free any associated resources.
 */
public interface Transaction extends AutoCloseable, ReadTransaction, TransactionContext {

	static public final int MATCH_INDEX_NOT_COMPATIBLE = -1;
	static public final int MATCH_INDEX_ALL = 0;
	static public final int MATCH_INDEX_NONE = 1;
	static public final int MATCH_INDEX_MATCHED_ONLY = 2;
	static public final int MATCH_INDEX_UNMATCHED_ONLY = 3;

	/**
	 * Adds a range of keys to the transaction's read conflict ranges as if you
	 * had read the range. As a result, other transactions that write a key in
	 * this range could cause the transaction to fail with a conflict.
	 *
	 * @param keyBegin the first key in the range (inclusive)
	 * @param keyEnd the ending key for the range (exclusive)
	 */
	void addReadConflictRange(byte[] keyBegin, byte[] keyEnd);

	/**
	 * Adds a key to the transaction's read conflict ranges as if you had read
	 * the key. As a result, other transactions that concurrently write this key
	 * could cause the transaction to fail with a conflict.
	 *
	 * @param key the key to be added to the read conflict range set
	 */
	void addReadConflictKey(byte[] key);

	/**
	 * Adds a range of keys to the transaction's write conflict ranges as if you
	 * had cleared the range. As a result, other transactions that concurrently
	 * read a key in this range could fail with a conflict.
	 *
	 * @param keyBegin the first key in the range (inclusive)
	 * @param keyEnd the ending key for the range (exclusive)
	 */
	void addWriteConflictRange(byte[] keyBegin, byte[] keyEnd);

	/**
	 * Adds a key to the transaction's write conflict ranges as if you had
	 * written the key. As a result, other transactions that concurrently read
	 * this key could fail with a conflict.
	 *
	 * @param key the key to be added to the range
	 */
	void addWriteConflictKey(byte[] key);

	/**
	 * Sets the value for a given key. This will not affect the
	 * database until {@link #commit} is called.
	 *
	 * @param key the key whose value is to be set
	 * @param value the value to set in the database
	 *
	 * @throws IllegalArgumentException if {@code key} or {@code value} is {@code null}
	 * @throws FDBException if the set operation otherwise fails
	 */
	void set(byte[] key, byte[] value);

	/**
	 * Clears a given key from the database. This will not affect the
	 * database until {@link #commit} is called.
	 *
	 * @param key the key whose value is to be cleared
	 *
	 * @throws IllegalArgumentException if {@code key} is {@code null}
	 * @throws FDBException if clear operation otherwise fails
	 */
	void clear(byte[] key);

	/**
	 * Clears a range of keys in the database.  The upper bound of the range is
	 *  exclusive; that is, the key (if one exists) that is specified as the end
	 *  of the range will NOT be cleared as part of this operation. Range clears are
	 *  efficient with FoundationDB -- clearing large amounts of data will be fast.
	 *  This will not affect the database until {@link #commit} is called.
	 *
	 * @param beginKey the first clear
	 * @param endKey the key one past the last key to clear
	 *
	 * @throws IllegalArgumentException if {@code beginKey} or {@code endKey} is {@code null}
	 * @throws FDBException if the clear operation otherwise fails
	 */
	void clear(byte[] beginKey, byte[] endKey);

	/**
	 * Clears a range of keys in the database. The upper bound of the range is
	 *  exclusive; that is, the key (if one exists) that is specified as the end
	 *  of the range will NOT be cleared as part of this operation.  Range clears are
	 *  efficient with FoundationDB -- clearing large amounts of data will be fast.
	 *  However, this will not immediately free up disk - data for the deleted range
	 *  is cleaned up in the background.
	 *  This will not affect the database until {@link #commit} is called.
	 *  <br>
	 *  For purposes of computing the transaction size, only the begin and end keys of a clear range are counted.
	 *  The size of the data stored in the range does not count against the transaction size limit.
	 *
	 *
	 * @param range the range of keys to clear
	 *
	 * @throws FDBException if the clear operation fails
	 */
	void clear(Range range);

	/**
	 * Replace with calls to {@link #clear(Range)} with a parameter from a call to
	 *  {@link Range#startsWith(byte[])}.
	 *
	 * @param prefix the starting bytes from the keys to be cleared.
	 *
	 * @throws FDBException if the clear-range operation fails
	 */
	@Deprecated
	void clearRangeStartsWith(byte[] prefix);

	/**
	 * An atomic operation is a single database command that carries out several
	 * logical steps: reading the value of a key, performing a transformation on
	 * that value, and writing the result. Different atomic operations perform
	 * different transformations. Like other database operations, an atomic
	 * operation is used within a transaction.<br>
	 * <br>
	 * Atomic operations do not expose the current value of the key to the client
	 * but simply send the database the transformation to apply. In regard to
	 * conflict checking, an atomic operation is equivalent to a write without a
	 * read. It can only cause other transactions performing reads of the key
	 * to conflict.<br>
	 * <br>
	 * By combining these logical steps into a single, read-free operation,
	 * FoundationDB can guarantee that the transaction will not conflict due to
	 * the operation. This makes atomic operations ideal for operating on keys
	 * that are frequently modified. A common example is the use of a key-value
	 * pair as a counter.<br>
	 * <br>
	 * <b>Note:</b> If a transaction uses both an atomic operation and a serializable
	 * read on the same key, the benefits of using the atomic operation (for both
	 * conflict checking and performance) are lost.
	 *
	 * The behavior of each {@link MutationType} is documented at its definition.
	 *
	 * @param optype the operation to perform
	 * @param key the target of the operation
	 * @param param the value with which to modify the key
	 */
	void mutate(MutationType optype, byte[] key, byte[] param);

	/**
	 * Commit this {@code Transaction}. See notes in class description. Consider using
	 *  {@code Database}'s {@link Database#run(Function) run()} calls for managing
	 *  transactional access to FoundationDB.
	 *
	 * <p>
	 * As with other client/server databases, in some failure scenarios a client may
	 *  be unable to determine whether a transaction succeeded. In these cases, an
	 *  {@link FDBException} will be thrown with error code {@code commit_unknown_result} (1021).
	 *  The {@link #onError} function regards this exception as a retryable one, so
	 *  retry loops that don't specifically detect {@code commit_unknown_result} could end
	 *  up executing a transaction twice. For more information, see the FoundationDB
	 *  Developer Guide documentation.
	 * </p>
	 *
	 * <p>
	 * If any operation is performed on a transaction after a commit has been
	 *  issued but before it has returned, both the commit and the operation will
	 *  throw an error code {@code used_during_commit} (2017). In this case, all
	 *  subsequent operations on this transaction will throw this error.
	 * </p>
	 *
	 * @return a {@code CompletableFuture} that, when set without error, guarantees the
	 *  {@code Transaction}'s modifications committed durably to the
	 *  database. If the commit failed, it will throw an {@link FDBException}.
	 */
	CompletableFuture<Void> commit();

	/**
	 * Gets the version number at which a successful commit modified the database.
	 * This must be called only after the successful (non-error) completion of a call
	 * to {@link #commit()} on this {@code Transaction}, or the behavior is undefined.
	 * Read-only transactions do not modify the database when committed and will have
	 * a committed version of -1. Keep in mind that a transaction which reads keys and
	 * then sets them to their current values may be optimized to a read-only transaction.
	 *
	 * @return the database version at which the commit succeeded
	 */
	Long getCommittedVersion();

	/**
	 * Returns a future which will contain the versionstamp which was used by any versionstamp 
	 * operations in this transaction. The future will be ready only after the successful 
	 * completion of a call to {@link #commit()} on this {@code Transaction}. Read-only 
	 * transactions do not modify the database when committed and will result in the future 
	 * completing with an error. Keep in mind that a transaction which reads keys and then sets 
	 * them to their current values may be optimized to a read-only transaction.
	 *
	 * @return a future containing the versionstamp which was used for any versionstamp operations 
	 * in this transaction
	 */
	CompletableFuture<byte[]> getVersionstamp();

	/**
	 * Returns a future that will contain the approximated size of the commit, which is the
	 * summation of mutations, read conflict ranges, and write conflict ranges. This can be
	 * called multiple times before transaction commit.
	 *
	 * @return a future that will contain the approximated size of the commit.
	 */
	CompletableFuture<Long> getApproximateSize();

	/**
	 * Resets a transaction and returns a delayed signal for error recovery.  If the error
	 *  encountered by the {@code Transaction} could not be recovered from, the returned
	 *  {@code CompletableFuture} will be set to an error state.
	 *
	 * The current {@code Transaction} object will be invalidated by this call and will throw errors
	 *  when used. The newly reset {@code Transaction} will be returned through the {@code CompletableFuture}
	 *  if the error was retryable.
	 *
	 * If the error is not retryable, then no reset {@code Transaction} is returned, leaving this
	 *  {@code Transaction} permanently invalidated.
	 *
	 * @param e the error caught while executing get()s and set()s on this {@code Transaction}
	 * @return a {@code CompletableFuture} to be set with a reset {@code Transaction} object to retry the transaction
	 */
	CompletableFuture<Transaction> onError(Throwable e);

	/**
	 * Cancels the {@code Transaction}. All pending and any future uses of the
	 *  {@code Transaction} will throw an {@link RuntimeException}.
	 */
	void cancel();

	/**
	 * Creates a watch that will become ready when it reports a change to
	 * the value of the specified key.<br>
	 * <br>
	 * A watch's behavior is relative to the transaction that created it. A
	 * watch will report a change in relation to the key's value as readable by
	 * that transaction. The initial value used for comparison is either that of
	 * the transaction's read version or the value as modified by the transaction
	 * itself prior to the creation of the watch. If the value changes and then
	 * changes back to its initial value, the watch might not report the change.<br>
	 * <br>
	 * Until the transaction that created it has been committed, a watch will
	 * not report changes made by other transactions. In contrast, a watch
	 * will immediately report changes made by the transaction itself. Watches
	 * cannot be created if the transaction has set
	 * {@link TransactionOptions#setReadYourWritesDisable()}, and an attempt to do
	 * so will raise a {@code watches_disabled} exception.<br>
	 * <br>
	 * If the transaction used to create a watch encounters an exception during
	 * commit, then the watch will be set with that exception. A transaction whose
	 * commit result is unknown will set all of its watches with the
	 * {@code commit_unknown_result} exception. If an uncommitted transaction is
	 * reset via {@link #onError} or destroyed, then any watches it created will be set
	 * with the {@code transaction_cancelled} exception.<br>
	 * <br>
	 * By default, each database connection can have no more than 10,000 watches
	 * that have not yet reported a change. When this number is exceeded, an
	 * attempt to create a watch will raise a {@code too_many_watches} exception.
	 * Because a watch outlives the transaction that creates it, any watch that is no
	 * longer needed should be cancelled.<br>
	 * <br>
	 * <b>NOTE:</b> calling code <i>must</i> call {@link Transaction#commit()} for
	 *  the watch to be registered with the database.
	 *
	 * @param key the key to watch for changes in value
	 *
	 * @return a {@code CompletableFuture} that will become ready when the value changes
	 *
	 * @throws FDBException if too many watches have been created on this database. The
	 *  limit defaults to 10,000 and can be modified with a call to
	 *  {@link DatabaseOptions#setMaxWatches(long)}.
	 */
	CompletableFuture<Void> watch(byte[] key) throws FDBException;

	/**
	 * Returns the {@link Database} that this {@code Transaction} is interacting
	 * with.
	 *
	 * @return the {@link Database} object
	 */
	Database getDatabase();

	/**
	 * Run a function once against this {@code Transaction}. This call blocks while
	 *  user code is executing, returning the result of that code on completion.
	 *
	 * @param retryable the block of logic to execute against this {@code Transaction}
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return a result of the single call to {@code retryable}
	 */
	@Override
	<T> T run(Function<? super Transaction, T> retryable);

	/**
	 * Run a function once against this {@code Transaction}. This call returns
	 *  immediately with a {@code CompletableFuture} handle to the result.
	 *
	 * @param retryable the block of logic to execute against this {@code Transaction}
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return a {@code CompletableFuture} that will be set to the return value of {@code retryable}
	 */
	@Override
	<T> CompletableFuture<T> runAsync(
			Function<? super Transaction, ? extends CompletableFuture<T>> retryable);

	/**
	 * Close the {@code Transaction} object and release any associated resources. This must be called at
	 *  least once after the {@code Transaction} object is no longer in use. This can be called multiple
	 *  times, but care should be taken that it is not in use in another thread at the time of the call.
	 */
	@Override
	void close();
}
