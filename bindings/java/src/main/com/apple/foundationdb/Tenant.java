/*
 * Tenant.java
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

package com.apple.foundationdb;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A tenant represents a named key-space within a database that can be interacted with
 * transactionally.<br>
 * <br>
 * The simplest correct programs using tenants will make use of the methods defined
 *  in the {@link TransactionContext} interface. When used on a {@code Tenant} these
 *  methods will call {@code Transaction#commit()} after user code has been
 *  executed. These methods will not return successfully until {@code commit()} has
 *  returned successfully.<br>
 * <br>
 * <b>Note:</b> {@code Tenant} objects must be {@link #close closed} when no longer
 *  in use in order to free any associated resources.
 */
public interface Tenant extends AutoCloseable, TransactionContext {
	/**
	 * Creates a {@link Transaction} that operates on this {@code Tenant}.<br>
	 * <br>
	 * <b>Note:</b> Java transactions automatically set the {@link TransactionOptions#setUsedDuringCommitProtectionDisable}
	 *  option. This is because the Java bindings disallow use of {@code Transaction} objects after
	 *  {@link Transaction#onError} is called.
	 *
	 * @return a newly created {@code Transaction} that reads from and writes to this {@code Tenant}.
	 */
	default Transaction createTransaction() {
		return createTransaction(getExecutor());
	}

	/**
	 * Creates a {@link Transaction} that operates on this {@code Tenant} with the given {@link Executor}
	 * for asynchronous callbacks.
	 *
	 * @param e the {@link Executor} to use when executing asynchronous callbacks.
	 * @return a newly created {@code Transaction} that reads from and writes to this {@code Tenant}.
	 */
	Transaction createTransaction(Executor e);

	/**
	 * Creates a {@link Transaction} that operates on this {@code Tenant} with the given {@link Executor}
	 * for asynchronous callbacks.
	 *
	 * @param e the {@link Executor} to use when executing asynchronous callbacks.
	 * @param eventKeeper the {@link EventKeeper} to use when tracking instrumented calls for the transaction.
	 *
	 * @return a newly created {@code Transaction} that reads from and writes to this {@code Tenant}.
	 */
	Transaction createTransaction(Executor e, EventKeeper eventKeeper);

	/**
	 * Returns the name of this {@code Tenant}.
	 *
	 * @return the name of this {@code Tenant} as a byte string.
	 */
	byte[] getName();

	/**
	 * Runs a read-only transactional function against this {@code Tenant} with retry logic.
	 *  {@link Function#apply(Object) apply(ReadTransaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  FDBException (or any {@code Throwable} other than an {@code FDBException})
	 *  is thrown. This call is blocking -- this
	 *  method will not return until the {@code Function} has been called and completed without error.<br>
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this tenant
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return the result of the last run of {@code retryable}
	 */
	@Override
	default <T> T read(Function<? super ReadTransaction, T> retryable) {
		return read(retryable, getExecutor());
	}

	/**
	 * Runs a read-only transactional function against this {@code Tenant} with retry logic. Use
	 *  this formulation of {@link #read(Function)} if one wants to set a custom {@link Executor}
	 *  for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this tenant
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 * @param <T> the return type of {@code retryable}
	 * @return the result of the last run of {@code retryable}
	 * 
	 * @see #read(Function)
	 */
	<T> T read(Function<? super ReadTransaction, T> retryable, Executor e);

	/**
	 * Runs a read-only transactional function against this {@code Tenant} with retry logic.
	 *  {@link Function#apply(Object) apply(ReadTransaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  FDBException (or any {@code Throwable} other than an {@code FDBException})
	 *  is thrown. This call is non-blocking -- this
	 *  method will return immediately and with a {@link CompletableFuture} that will be
	 *  set when the {@code Function} has been called and completed without error.<br>
	 * <br>
	 * Any errors encountered executing {@code retryable}, or received from the
	 *  database, will be set on the returned {@code CompletableFuture}.
	 *
	 * @param retryable the block of logic to execute in a {@link ReadTransaction} against
	 *  this tenant
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
	 * Runs a read-only transactional function against this {@code Tenant} with retry logic.
	 *  Use this version of {@link #readAsync(Function)} if one wants to set a custom
	 *  {@link Executor} for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link ReadTransaction} against
	 *  this tenant
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
	 * Runs a transactional function against this {@code Tenant} with retry logic.
	 *  {@link Function#apply(Object) apply(Transaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  FDBException (or any {@code Throwable} other than an {@code FDBException})
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
	 *  this tenant
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return the result of the last run of {@code retryable}
	 */
	@Override
	default <T> T run(Function<? super Transaction, T> retryable) {
		return run(retryable, getExecutor());
	}

	/**
	 * Runs a transactional function against this {@code Tenant} with retry logic.
	 *  Use this formulation of {@link #run(Function)} if one would like to set a
	 *  custom {@link Executor} for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this tenant
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 * @param <T> the return type of {@code retryable}
	 *
	 * @return the result of the last run of {@code retryable}
	 */
	<T> T run(Function<? super Transaction, T> retryable, Executor e);

	/**
	 * Runs a transactional function against this {@code Tenant} with retry logic.
	 *  {@link Function#apply(Object) apply(Transaction)} will be called on the
	 *  supplied {@link Function} until a non-retryable
	 *  FDBException (or any {@code Throwable} other than an {@code FDBException})
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
	 *  this tenant
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
	 * Runs a transactional function against this {@code Tenant} with retry logic. Use
	 *  this formulation of the non-blocking {@link #runAsync(Function)} if one wants
	 *  to set a custom {@link Executor} for the transaction when run.
	 *
	 * @param retryable the block of logic to execute in a {@link Transaction} against
	 *  this tenant
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
	 * Runs {@link #getId()} on the default executor.
	 *
	 * @return a future with the tenant ID
	 */
	default CompletableFuture<Long> getId() {
		return getId(getExecutor());
	}

	/**
	 * Returns the tenant ID of this tenant.
	 *
	 * @param e the {@link Executor} to use for asynchronous callbacks
	 *
	 * @return a future with the tenant ID
	 */
	CompletableFuture<Long> getId(Executor e);

	/**
	 * Close the {@code Tenant} object and release any associated resources. This must be called at
	 *  least once after the {@code Tenant} object is no longer in use. This can be called multiple
	 *  times, but care should be taken that it is not in use in another thread at the time of the call.
	 */
	@Override
	void close();
}
