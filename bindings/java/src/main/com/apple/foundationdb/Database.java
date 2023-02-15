/*
 * Database.java
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
import java.util.concurrent.Executor;
import java.util.function.Function;
import com.apple.foundationdb.tuple.Tuple;

/**
 * A mutable, lexicographically ordered mapping from binary keys to binary values.
 *  {@link Transaction}s are used to manipulate data within a single
 *  {@code Database} -- multiple, concurrent
 *  {@code Transaction}s on a {@code Database} enforce <b>ACID</b> properties.<br>
 * <br>
 * The simplest correct programs using FoundationDB will make use of the methods defined
 *  in the {@link TransactionContext} interface. When used on a {@code Database} these
 *  methods will call {@code Transaction#commit()} after user code has been
 *  executed. These methods will not return successfully until {@code commit()} has
 *  returned successfully.<br>
 * <br>
 * <b>Note:</b> {@code Database} objects must be {@link #close closed} when no longer
 *  in use in order to free any associated resources.
 */
public interface Database extends AutoCloseable, TransactionContext {
	/**
	 * Opens an existing tenant to be used for running transactions.<br>
	 * <br>
	 * <b>Note:</b> opening a tenant does not check its existence in the cluster. If the tenant does not exist,
	 * attempts to read or write data with it will fail.
	 *
	 * @param tenantName The name of the tenant to open.
	 * @return a {@link Tenant} that can be used to create transactions that will operate in the tenant's key-space.
	 */
	default Tenant openTenant(byte[] tenantName) {
		return openTenant(tenantName, getExecutor());
	}

	/**
	 * Opens an existing tenant to be used for running transactions. This is a convenience method that generates the
	 * tenant name by packing a {@code Tuple}.<br>
	 * <br>
	 * <b>Note:</b> opening a tenant does not check its existence in the cluster. If the tenant does not exist,
	 * attempts to read or write data with it will fail.
	 *
	 * @param tenantName The name of the tenant to open, as a Tuple.
	 * @return a {@link Tenant} that can be used to create transactions that will operate in the tenant's key-space.
	 */
	Tenant openTenant(Tuple tenantName);

	/**
	 * Opens an existing tenant to be used for running transactions.
	 *
	 * @param tenantName The name of the tenant to open.
	 * @param e the {@link Executor} to use when executing asynchronous callbacks.
	 * @return a {@link Tenant} that can be used to create transactions that will operate in the tenant's key-space.
	 */
	Tenant openTenant(byte[] tenantName, Executor e);

	/**
	 * Opens an existing tenant to be used for running transactions. This is a convenience method that generates the
	 * tenant name by packing a {@code Tuple}.
	 *
	 * @param tenantName The name of the tenant to open, as a Tuple.
	 * @param e the {@link Executor} to use when executing asynchronous callbacks.
	 * @return a {@link Tenant} that can be used to create transactions that will operate in the tenant's key-space.
	 */
	Tenant openTenant(Tuple tenantName, Executor e);

	/**
	 * Opens an existing tenant to be used for running transactions.
	 *
	 * @param tenantName The name of the tenant to open.
	 * @param e the {@link Executor} to use when executing asynchronous callbacks.
	 * @param eventKeeper the {@link EventKeeper} to use when tracking instrumented calls for the tenant's transactions.
	 * @return a {@link Tenant} that can be used to create transactions that will operate in the tenant's key-space.
	 */
	Tenant openTenant(byte[] tenantName, Executor e, EventKeeper eventKeeper);

	/**
	 * Opens an existing tenant to be used for running transactions. This is a convenience method that generates the
	 * tenant name by packing a {@code Tuple}.
	 *
	 * @param tenantName The name of the tenant to open, as a Tuple.
	 * @param e the {@link Executor} to use when executing asynchronous callbacks.
	 * @param eventKeeper the {@link EventKeeper} to use when tracking instrumented calls for the tenant's transactions.
	 * @return a {@link Tenant} that can be used to create transactions that will operate in the tenant's key-space.
	 */
	Tenant openTenant(Tuple tenantName, Executor e, EventKeeper eventKeeper);

	/**
	 * Creates a {@link Transaction} that operates on this {@code Database}. Creating a transaction
	 *  in this way does not associate it with a {@code Tenant}, and as a result the transaction will
	 *  operate on the entire key-space for the database.<br>
	 * <br>
	 * <b>Note:</b> Java transactions automatically set the {@link TransactionOptions#setUsedDuringCommitProtectionDisable}
	 *  option. This is because the Java bindings disallow use of {@code Transaction} objects after
	 *  {@link Transaction#onError} is called.<br>
	 * <br>
	 * <b>Note:</b> Transactions created directly on a {@code Database} object cannot be used in a cluster
	 *  that requires tenant-based access. To run transactions in those clusters, you must first open a tenant
	 *  with {@link #openTenant(byte[])}.
	 *
	 * @return a newly created {@code Transaction} that reads from and writes to this {@code Database}.
	 */
	default Transaction createTransaction() {
		return createTransaction(getExecutor());
	}

	/**
	 * Creates a {@link Transaction} that operates on this {@code Database} with the given {@link Executor}
	 * for asynchronous callbacks.
	 *
	 * @param e the {@link Executor} to use when executing asynchronous callbacks for the database
	 * @return a newly created {@code Transaction} that reads from and writes to this {@code Database}.
	 */
	Transaction createTransaction(Executor e);

	/**
	 * Creates a {@link Transaction} that operates on this {@code Database} with the given {@link Executor}
	 * for asynchronous callbacks.
	 *
	 * @param e the {@link Executor} to use when executing asynchronous callbacks for the database
	 * @param eventKeeper the {@link EventKeeper} to use when tracking instrumented calls for the transaction.
	 *
	 * @return a newly created {@code Transaction} that reads from and writes to this {@code Database}.
	 */
	Transaction createTransaction(Executor e, EventKeeper eventKeeper);

	/**
	 * Returns a set of options that can be set on a {@code Database}
	 *
	 * @return a set of database-specific options affecting this {@code Database}
	 */
	DatabaseOptions options();

	/**
	 * Returns a value which indicates the saturation of the client
	 * <br>
	 * <b>Note:</b> By default, this value is updated every second
	 *
	 * @return a value where 0 indicates that the client is idle and 1 (or larger) indicates that the client is saturated.
	 */
	double getMainThreadBusyness();

	/**
	 * Runs {@link #purgeBlobGranules(byte[] beginKey, byte[] endKey, boolean force)} on the default executor.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param force if true delete all data, if not keep data &gt;= purgeVersion
	 *
	 * @return the key to watch for purge complete
	 */
	default CompletableFuture<byte[]> purgeBlobGranules(byte[] beginKey, byte[] endKey, boolean force) {
		return purgeBlobGranules(beginKey, endKey, -2, force, getExecutor());
	}

	/**
	 * Runs {@link #purgeBlobGranules(byte[] beginKey, byte[] endKey, long purgeVersion, boolean force)} on the default executor.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param purgeVersion version to purge at
	 * @param force if true delete all data, if not keep data &gt;= purgeVersion
	 *
	 * @return the key to watch for purge complete
	 */
	default CompletableFuture<byte[]> purgeBlobGranules(byte[] beginKey, byte[] endKey, long purgeVersion, boolean force) {
		return purgeBlobGranules(beginKey, endKey, purgeVersion, force, getExecutor());
	}

	/**
	 * Queues a purge of blob granules for the specified key range, at the specified version.
     *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param purgeVersion version to purge at
	 * @param force if true delete all data, if not keep data &gt;= purgeVersion
	 * @param e the {@link Executor} to use for asynchronous callbacks

	 * @return the key to watch for purge complete
	 */
	CompletableFuture<byte[]> purgeBlobGranules(byte[] beginKey, byte[] endKey, long purgeVersion, boolean force, Executor e);


	/**
	 * Runs {@link #waitPurgeGranulesComplete(byte[] purgeKey)} on the default executor.
	 *
	 * @param purgeKey key to watch
	 *
	 * @return void
	 */
	default CompletableFuture<Void> waitPurgeGranulesComplete(byte[] purgeKey) {
		return waitPurgeGranulesComplete(purgeKey, getExecutor());
	}

	/**
	 * Wait for a previous call to purgeBlobGranules to complete.
	 *
	 * @param purgeKey key to watch
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 *
	 * @return void
	 */
	CompletableFuture<Void> waitPurgeGranulesComplete(byte[] purgeKey, Executor e);

	/**
	 * Runs {@link #blobbifyRange(byte[] beginKey, byte[] endKey)} on the default executor.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range

	 * @return if the recording of the range was successful
	 */
	default CompletableFuture<Boolean> blobbifyRange(byte[] beginKey, byte[] endKey) {
		return blobbifyRange(beginKey, endKey, getExecutor());
	}

	/**
	 * Sets a range to be blobbified in the database. Must be a completely unblobbified range.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param e the {@link Executor} to use for asynchronous callbacks

	 * @return if the recording of the range was successful
	 */
	CompletableFuture<Boolean> blobbifyRange(byte[] beginKey, byte[] endKey, Executor e);

	/**
	 * Runs {@link #unblobbifyRange(byte[] beginKey, byte[] endKey)} on the default executor.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range

	 * @return if the recording of the range was successful
	 */
	default CompletableFuture<Boolean> unblobbifyRange(byte[] beginKey, byte[] endKey) {
		return unblobbifyRange(beginKey, endKey, getExecutor());
	}

	/**
	 * Unsets a blobbified range in the database. The range must be aligned to known blob ranges.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param e the {@link Executor} to use for asynchronous callbacks

	 * @return if the recording of the range was successful
	 */
	CompletableFuture<Boolean> unblobbifyRange(byte[] beginKey, byte[] endKey, Executor e);

	/**
	 * Runs {@link #listBlobbifiedRanges(byte[] beginKey, byte[] endKey, int rangeLimit)} on the default executor.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param rangeLimit batch size

	 * @return a future with the list of blobbified ranges: [lastLessThan(beginKey), firstGreaterThanOrEqual(endKey)]
	 */
	 default CompletableFuture<KeyRangeArrayResult> listBlobbifiedRanges(byte[] beginKey, byte[] endKey, int rangeLimit) {
		return listBlobbifiedRanges(beginKey, endKey, rangeLimit, getExecutor());
	 }

	/**
	 * Lists blobbified ranges in the database. There may be more if result.size() == rangeLimit.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param rangeLimit batch size
	 * @param e the {@link Executor} to use for asynchronous callbacks

	 * @return a future with the list of blobbified ranges: [lastLessThan(beginKey), firstGreaterThanOrEqual(endKey)]
	 */
	 CompletableFuture<KeyRangeArrayResult> listBlobbifiedRanges(byte[] beginKey, byte[] endKey, int rangeLimit, Executor e);

	/**
	 * Runs {@link #verifyBlobRange(byte[] beginKey, byte[] endKey)} on the default executor.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 *
	 * @return a future with the version of the last blob granule.
	 */
	default CompletableFuture<Long> verifyBlobRange(byte[] beginKey, byte[] endKey) {
		return verifyBlobRange(beginKey, endKey, -2, getExecutor());
	}

	/**
	 * Runs {@link #verifyBlobRange(byte[] beginKey, byte[] endKey, long version)} on the default executor.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param version version to read at
	 *
	 * @return a future with the version of the last blob granule.
	 */
	default CompletableFuture<Long> verifyBlobRange(byte[] beginKey, byte[] endKey, long version) {
		return verifyBlobRange(beginKey, endKey, version, getExecutor());
	}

	/**
	 * Checks if a blob range is blobbified.
	 *
	 * @param beginKey start of the key range
	 * @param endKey end of the key range
	 * @param version version to read at
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 *
	 * @return a future with the version of the last blob granule.
	 */
	CompletableFuture<Long> verifyBlobRange(byte[] beginKey, byte[] endKey, long version, Executor e);

	/**
	 * Runs a read-only transactional function against this {@code Database} with retry logic.
	 *  {@link Function#apply(Object) apply(ReadTransaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  {@link FDBException} (or any {@code Throwable} other than an {@code FDBException})
	 *  is thrown. This call is blocking -- this
	 *  method will not return until the {@code Function} has been called and completed without error.<br>
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this database
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return the result of the last run of {@code retryable}
	 */
	@Override
	default <T> T read(Function<? super ReadTransaction, T> retryable) {
		return read(retryable, getExecutor());
	}

	/**
	 * Runs a read-only transactional function against this {@code Database} with retry logic. Use
	 *  this formulation of {@link #read(Function)} if one wants to set a custom {@link Executor}
	 *  for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this database
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 * @param <T> the return type of {@code retryable}
	 * @return the result of the last run of {@code retryable}
	 * 
	 * @see #read(Function)
	 */
	<T> T read(Function<? super ReadTransaction, T> retryable, Executor e);

	/**
	 * Runs a read-only transactional function against this {@code Database} with retry logic.
	 *  {@link Function#apply(Object) apply(ReadTransaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  {@link FDBException} (or any {@code Throwable} other than an {@code FDBException})
	 *  is thrown. This call is non-blocking -- this
	 *  method will return immediately and with a {@link CompletableFuture} that will be
	 *  set when the {@code Function} has been called and completed without error.<br>
	 * <br>
	 * Any errors encountered executing {@code retryable}, or received from the
	 *  database, will be set on the returned {@code CompletableFuture}.
	 *
	 * @param retryable the block of logic to execute in a {@link ReadTransaction} against
	 *  this database
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return a {@code CompletableFuture} that will be set to the value returned by the last call
	 *  to {@code retryable}
	 */
	@Override
	default <T> CompletableFuture<T> readAsync(
			Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable) {
		return readAsync(retryable, getExecutor());
	}

	/**
	 * Runs a read-only transactional function against this {@code Database} with retry logic.
	 *  Use this version of {@link #readAsync(Function)} if one wants to set a custom
	 *  {@link Executor} for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link ReadTransaction} against
	 *  this database
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return a {@code CompletableFuture} that will be set to the value returned by the last call
	 *  to {@code retryable}
	 *
	 * @see #readAsync(Function)
	 */
	<T> CompletableFuture<T> readAsync(
			Function<? super ReadTransaction, ? extends CompletableFuture<T>> retryable, Executor e);

	/**
	 * Runs a transactional function against this {@code Database} with retry logic.
	 *  {@link Function#apply(Object) apply(Transaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  {@link FDBException} (or any {@code Throwable} other than an {@code FDBException})
	 *  is thrown or {@link Transaction#commit() commit()},
	 *  when called after {@code apply()}, returns success. This call is blocking -- this
	 *  method will not return until {@code commit()} has been called and returned success.<br>
	 * <br>
	 * As with other client/server databases, in some failure scenarios a client may
	 *  be unable to determine whether a transaction succeeded. In these cases, your
	 *  transaction may be executed twice. For more information about how to reason
	 *  about these situations see
	 * <a href="/foundationdb/developer-guide.html#transactions-with-unknown-results"
	 *   target="_blank">the FounationDB Developer Guide</a>
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this database
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return the result of the last run of {@code retryable}
	 */
	@Override
	default <T> T run(Function<? super Transaction, T> retryable) {
		return run(retryable, getExecutor());
	}

	/**
	 * Runs a transactional function against this {@code Database} with retry logic.
	 *  Use this formulation of {@link #run(Function)} if one would like to set a
	 *  custom {@link Executor} for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this database
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return the result of the last run of {@code retryable}
	 */
	<T> T run(Function<? super Transaction, T> retryable, Executor e);

	/**
	 * Runs a transactional function against this {@code Database} with retry logic.
	 *  {@link Function#apply(Object) apply(Transaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  {@link FDBException} (or any {@code Throwable} other than an {@code FDBException})
	 *  is thrown or {@link Transaction#commit() commit()},
	 *  when called after {@code apply()}, returns success. This call is non-blocking -- this
	 *  method will return immediately and with a {@link CompletableFuture} that will be
	 *  set when {@code commit()} has been called and returned success.<br>
	 * <br>
	 * As with other client/server databases, in some failure scenarios a client may
	 *  be unable to determine whether a transaction succeeded. In these cases, your
	 *  transaction may be executed twice. For more information about how to reason
	 *  about these situations see
	 * <a href="/foundationdb/developer-guide.html#transactions-with-unknown-results"
	 *   target="_blank">the FounationDB Developer Guide</a><br>
	 * <br>
	 * Any errors encountered executing {@code retryable}, or received from the
	 *  database, will be set on the returned {@code CompletableFuture}.
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this database
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return a {@code CompletableFuture} that will be set to the value returned by the last call
	 *  to {@code retryable}
	 */
	@Override
	default <T> CompletableFuture<T> runAsync(
			Function<? super Transaction, ? extends CompletableFuture<T>> retryable) {
		return runAsync(retryable, getExecutor());
	}

	/**
	 * Runs a transactional function against this {@code Database} with retry logic. Use
	 *  this formulation of the non-blocking {@link #runAsync(Function)} if one wants
	 *  to set a custom {@link Executor} for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this database
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return a {@code CompletableFuture} that will be set to the value returned by the last call
	 *  to {@code retryable}
	 *
	 * @see #run(Function)
	 */
	<T> CompletableFuture<T> runAsync(
			Function<? super Transaction, ? extends CompletableFuture<T>> retryable, Executor e);

	/**
	 * Close the {@code Database} object and release any associated resources. This must be called at
	 *  least once after the {@code Database} object is no longer in use. This can be called multiple
	 *  times, but care should be taken that it is not in use in another thread at the time of the call.
	 */
	@Override
	void close();

	/**
	 * Returns client-side status information
	 *
	 * @return a {@code CompletableFuture} containing a JSON string with client status health information
	 */
	default CompletableFuture<byte[]> getClientStatus() {
		return getClientStatus(getExecutor());
	}

	/**
	 * Returns client-side status information
	 *
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 * @return a {@code CompletableFuture} containing a JSON string with client status health information
	 */
	CompletableFuture<byte[]> getClientStatus(Executor e);
}
