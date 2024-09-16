/*
 * AsyncUtil.java
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

package com.apple.foundationdb.async;

import static com.apple.foundationdb.FDB.DEFAULT_EXECUTOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provided utilities for using and manipulating {@link CompletableFuture}s.
 */
public class AsyncUtil {
	/**
	 * A completed future of type {@link Void}. In particular, it is completed to {@code null},
	 *  but that shouldn't really matter for the {@link Void} type. This can be used instead
	 *  of creating a new future if one wants to signal that some asynchronous task has
	 *  already been completed.
	 */
	public static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);
	/**
	 * A completed future of type {@link Boolean} that is set to {@code true}. This can be
	 *  used instead of creating a new future if one wants to signal that some task has
	 *  already been completed with a {@code true} result.
	 */
	public static final CompletableFuture<Boolean> READY_TRUE = CompletableFuture.completedFuture(Boolean.TRUE);
	/**
	 * A completed future of type {@link Boolean} that is set to {@code false}. This can be
	 *  used instead of creating a new future if one wants to signal that some task has
	 *  already been completed with a {@code false} result.
	 */
	public static final CompletableFuture<Boolean> READY_FALSE = CompletableFuture.completedFuture(Boolean.FALSE);

	/**
	 * Run {@code Function} {@code func}, returning all caught exceptions as a
	 *  {@code CompletableFuture} in an error state.
	 *
	 * @param func the {@code Function} to run
	 * @param value the input to pass to {@code func}
	 * @param <I> type of input to {@code func}
	 * @param <O> type of output of {@code func}
	 *
	 * @return the output of {@code func}, or a {@code CompletableFuture} carrying any exception
	 *  caught in the process.
	 */
	public static <I,O> CompletableFuture<O> applySafely(Function<I, ? extends CompletableFuture<O>> func, I value) {
		try {
			return func.apply(value);
		} catch (RuntimeException e) {
			CompletableFuture<O> future = new CompletableFuture<>();
			future.completeExceptionally(e);
			return future;
		}
	}

	/**
	 * Run the {@code consumer} on each element of the iterable in order. The future will
	 *  complete with either the first error encountered by either the iterable itself
	 *  or by the consumer provided or with {@code null} if the future completes
	 *  successfully. Items are processed in order from the iterable, and each item
	 *  will be processed only after the item before it has finished processing.
	 *
	 * @param iterable the source of data over from which to consume
	 * @param consumer operation to apply to each item
	 * @param <V> type of the items returned by the iterable
	 *
	 * @return a future that is ready once the asynchronous operation completes
	 */
	public static <V> CompletableFuture<Void> forEach(final AsyncIterable<V> iterable, final Consumer<? super V> consumer) {
		return forEachRemaining(iterable.iterator(), consumer);
	}

	/**
	 * Run the {@code consumer} on each element of the iterable in order. The future will
	 *  complete with either the first error encountered by either the iterable itself
	 *  or by the consumer provided or with {@code null} if the future completes
	 *  successfully. Items are processed in order from the iterable, and each item
	 *  will be processed only after the item before it has finished processing. Asynchronous
	 *  tasks needed to complete this operation are scheduled on the provided executor.
	 *
	 * @param iterable the source of data over from which to consume
	 * @param consumer operation to apply to each item
	 * @param executor executor on which to schedule asynchronous tasks
	 * @param <V> type of the items returned by the iterable
	 *
	 * @return a future that is ready once the asynchronous operation completes
	 */
	public static <V> CompletableFuture<Void> forEach(final AsyncIterable<V> iterable, final Consumer<? super V> consumer, final Executor executor) {
		return forEachRemaining(iterable.iterator(), consumer, executor);
	}

	/**
	 * Run the {@code consumer} on each element remaining in the iterator in order. The future will
	 *  complete with either the first error encountered by either the iterator itself
	 *  or by the consumer provided or with {@code null} if the future completes
	 *  successfully. Items are processed in order from the iterator, and each item
	 *  will be processed only after the item before it has finished processing.
	 *
	 * @param iterator the source of data over from which to consume
	 * @param consumer operation to apply to each item
	 * @param <V> type of the items returned by the iterator
	 *
	 * @return a future that is ready once the asynchronous operation completes
	 */
	public static <V> CompletableFuture<Void> forEachRemaining(final AsyncIterator<V> iterator, final Consumer<? super V> consumer) {
		return forEachRemaining(iterator, consumer, DEFAULT_EXECUTOR);
	}

	/**
	 * Run the {@code consumer} on each element remaining if the iterator in order. The future will
	 *  complete with either the first error encountered by either the iterator itself
	 *  or by the consumer provided or with {@code null} if the future completes
	 *  successfully. Items are processed in order from the iterator, and each item
	 *  will be processed only after the item before it has finished processing. Asynchronous
	 *  tasks needed to complete this operation are scheduled on the provided executor.
	 *
	 * @param iterator the source of data over from which to consume
	 * @param consumer operation to apply to each item
	 * @param executor executor on which to schedule asynchronous tasks
	 * @param <V> type of the items returned by the iterator
	 *
	 * @return a future that is ready once the asynchronous operation completes
	 */
	public static <V> CompletableFuture<Void> forEachRemaining(final AsyncIterator<V> iterator, final Consumer<? super V> consumer, final Executor executor) {
		return iterator.onHasNext().thenComposeAsync(hasAny -> {
			if (hasAny) {
				return whileTrue(() -> {
					consumer.accept(iterator.next());
					return iterator.onHasNext();
				}, executor);
			} else {
				return DONE;
			}
		}, executor);
	}

	/**
	 * Iterates over a stream of items and returns the result as a list.
	 *
	 * @param iterable the source of data over which to iterate
	 * @param <V> type of the items returned by the iterable
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collect(final AsyncIterable<V> iterable) {
		return collect(iterable, DEFAULT_EXECUTOR);
	}

	/**
	 * Iterates over a set of items and returns the remaining results as a list.
	 *
	 * @param iterator the source of data over which to iterate. This function will exhaust the iterator.
	 * @param <V> type of the items returned by the iterator
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collectRemaining(final AsyncIterator<V> iterator) {
		return collectRemaining(iterator, DEFAULT_EXECUTOR);
	}

	/**
	 * Iterates over a set of items and returns the result as a list.
	 *
	 * @param iterable the source of data over which to iterate
	 * @param executor the {@link Executor} to use for asynchronous operations
	 * @param <V> type of the items returned by the iterable
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collect(final AsyncIterable<V> iterable, final Executor executor) {
		return collectRemaining(iterable.iterator(), executor);
	}

	/**
	 * Iterates over a set of items and returns the remaining results as a list.
	 *
	 * @param iterator the source of data over which to iterate. This function will exhaust the iterator.
	 * @param executor the {@link Executor} to use for asynchronous operations
	 * @param <V> type of the items returned by the iterator
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collectRemaining(final AsyncIterator<V> iterator, final Executor executor) {
		final List<V> accumulator = new LinkedList<>();
		return tag(forEachRemaining(iterator, accumulator::add, executor), accumulator);
	}

	/**
	 * Map an {@code AsyncIterable} into an {@code AsyncIterable} of another type or with
	 *  each element modified in some fashion.
	 *
	 * @param iterable input
	 * @param func mapping function applied to each element
	 * @param <V> type of the items returned by the original iterable
	 * @param <T> type of the items returned by the final iterable
	 *
	 * @return a new iterable with each element mapped to a different value
	 */
	public static <V, T> AsyncIterable<T> mapIterable(final AsyncIterable<V> iterable,
			final Function<V, T> func) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> iterator() {
				return mapIterator(iterable.iterator(), func);
			}

			@Override
			public CompletableFuture<List<T>> asList() {
				final List<T> accumulator = new LinkedList<>();
				return tag(AsyncUtil.forEach(iterable, value -> accumulator.add(func.apply(value))), accumulator);
			}
		};
	}

	/**
	 * Map an {@code AsyncIterator} into an {@code AsyncIterator} of another type or with
	 *  each element modified in some fashion.
	 *
	 * @param iterator input
	 * @param func mapping function applied to each element
	 * @param <V> type of the items returned by the original iterator
	 * @param <T> type of the items returned by the final iterator
	 *
	 * @return a new iterator with each element mapped to a different value
	 */
	public static <V, T> AsyncIterator<T> mapIterator(final AsyncIterator<V> iterator,
													  final Function<V, T> func) {
		return new AsyncIterator<T>() {
			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public CompletableFuture<Boolean> onHasNext() {
				return iterator.onHasNext();
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public T next() {
				return func.apply(iterator.next());
			}

			@Override
			public void cancel() {
				iterator.cancel();
			}
		};
	}

	/**
	 * Map a {@code CloseableAsyncIterator} into a {@code CloseableAsyncIterator} of another type or with
	 *  each element modified in some fashion.
	 *
	 * @param iterator input
	 * @param func mapping function applied to each element
	 * @param <V> type of the items returned by the original iterator
	 * @param <T> type of the items returned by the final iterator
	 *
	 * @return a new iterator with each element mapped to a different value
	 */
	public static <V, T> CloseableAsyncIterator<T> mapIterator(final CloseableAsyncIterator<V> iterator,
													  final Function<V, T> func) {
		return new CloseableAsyncIterator<T>() {
			@Override
			public void remove() {
				iterator.remove();
			}

			@Override
			public CompletableFuture<Boolean> onHasNext() {
				return iterator.onHasNext();
			}

			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public T next() {
				return func.apply(iterator.next());
			}

			@Override
			public void cancel() {
				iterator.cancel();
			}

			@Override
			public void close() {
				iterator.close();
			}
		};
	}

	private static class LoopPartial implements BiFunction<Boolean, Throwable, Void> {
		final Supplier<? extends CompletableFuture<Boolean>> body;
		final CompletableFuture<Void> done;
		final Executor executor;

		LoopPartial(Supplier<? extends CompletableFuture<Boolean>> body, Executor executor) {
			this.body = body;
			this.done = new CompletableFuture<>();
			this.executor = executor;
		}

		@Override
		public Void apply(Boolean more, Throwable error) {
			if (error != null) {
				done.completeExceptionally(error);
			} else {
				while (true) {
					if (!more) {
						done.complete(null);
						break;
					}
					CompletableFuture<Boolean> result;
					try {
						result = body.get();
					} catch (Exception e) {
						done.completeExceptionally(e);
						break;
					}
					if (result.isDone()) {
						if (result.isCompletedExceptionally()) {
							result.handle(this);
							break;
						} else {
							more = result.join();
						}
					} else {
						result.handleAsync(this, executor);
						break;
					}
				}
			}

			return null;
		}

		public CompletableFuture<Void> run() {
			apply(true, null);
			return done;
		}
	}

	/**
	 * Executes an asynchronous operation repeatedly until it returns {@code False}.
	 *
	 * @param body the asynchronous operation over which to loop
	 *
	 * @return a {@link CompletableFuture} which will be set at completion of the loop.
	 * @deprecated Since version 5.1.0. Use the version of {@link #whileTrue(Supplier) whileTrue} that takes a
	 * {@link Supplier} instead.
	 */
	@Deprecated
	public static CompletableFuture<Void> whileTrue(Function<Void,? extends CompletableFuture<Boolean>> body) {
		return whileTrue(body, DEFAULT_EXECUTOR);
	}

	/**
	 * Executes an asynchronous operation repeatedly until it returns {@code False}.
	 *
	 * @param body the asynchronous operation over which to loop
	 * @param executor the {@link Executor} to use for asynchronous operations
	 *
	 * @return a {@link CompletableFuture} which will be set at completion of the loop.
	 * @deprecated Since version 5.1.0. Use the version of {@link #whileTrue(Supplier, Executor) whileTrue} that takes a
	 * {@link Supplier} instead.
	 */
	@Deprecated
	public static CompletableFuture<Void> whileTrue(Function<Void,? extends CompletableFuture<Boolean>> body, Executor executor) {
		return whileTrue(() -> body.apply(null), executor);
	}

	/**
	 * Executes an asynchronous operation repeatedly until it returns {@code False}.
	 *
	 * @param body the asynchronous operation over which to loop
	 *
	 * @return a {@link CompletableFuture} which will be set at completion of the loop.
	 */
	public static CompletableFuture<Void> whileTrue(Supplier<CompletableFuture<Boolean>> body) {
		return whileTrue(body, DEFAULT_EXECUTOR);
	}

	/**
	 * Executes an asynchronous operation repeatedly until it returns {@code False}.
	 *
	 * @param body the asynchronous operation over which to loop
	 * @param executor the {@link Executor} to use for asynchronous operations
	 *
	 * @return a {@link CompletableFuture} which will be set at completion of the loop.
	 */
	public static CompletableFuture<Void> whileTrue(Supplier<CompletableFuture<Boolean>> body, Executor executor) {
		return new LoopPartial(body, executor).run();
	}

	/**
	 * Maps the outcome of a task into a completion signal. Can be useful if {@code task} has
	 *  side-effects for which all is needed is a signal of completion.
	 *  All errors from {@code task} will be passed to the resulting {@code CompletableFuture}.
	 *
	 * @param task the asynchronous process for which to signal completion
	 * @param <V> type of element returned by {@code task}
	 *
	 * @return a newly created {@code CompletableFuture} that is set when {@code task} completes
	 */
	public static <V> CompletableFuture<Void> success(CompletableFuture<V> task) {
		return task.thenApply(o -> null);
	}

	/**
	 * Maps the readiness of a {@link CompletableFuture} into a completion signal.  When
	 *  the given {@link CompletableFuture} is set to a value or an error, the returned {@link CompletableFuture}
	 *  will be set to null.  The returned {@link CompletableFuture} will never be set to an error unless
	 *  it is explicitly cancelled.
	 *
	 * @param task the asynchronous process to monitor the readiness of
	 * @param <V> return type of the asynchronous task
	 *
	 * @return a new {@link CompletableFuture} that is set when {@code task} is ready.
	 */
	public static <V> CompletableFuture<Void> whenReady(CompletableFuture<V> task) {
		return task.handle((v, t) -> null);
	}

	/**
	 * Composes an asynchronous task with an exception-handler that returns a {@link CompletableFuture}
	 *  of the same type. If {@code task} completes normally, this will return a {@link CompletableFuture}
	 *  with the same value as {@code task}. If {@code task} completes exceptionally,
	 *  this will call {@code fn} with the exception returned by {@code task} and return
	 *  the result of the {@link CompletableFuture} returned by that function.
	 *
	 * @param task the asynchronous process to handle exceptions from
	 * @param fn a function mapping exceptions from {@code task} to a {@link CompletableFuture} of the same
	 *           type as {@code task}
	 * @param <V> return type of the asynchronous task
	 *
	 * @return a {@link CompletableFuture} that contains the value returned by {@code task}
	 *         if {@code task} completes normally and the result of {@code fn} if {@code task}
	 *         completes exceptionally
	 */
	public static <V> CompletableFuture<V> composeExceptionally(CompletableFuture<V> task, Function<Throwable, CompletableFuture<V>> fn) {
		return task.handle((v,e) -> e)
				.thenCompose(e -> {
					if (e != null) {
						return fn.apply(e);
					} else {
						return task;
					}
				});
	}

	/**
	 * Compose a handler bi-function to the result of a future. Unlike the
	 *  {@link CompletableFuture#handle(BiFunction) CompletableFuture.handle()}
	 *  function, which requires that the handler return a regular value, this
	 *  method requires that the handler return a {@link CompletableFuture}.
	 *  The returned future will then be ready with the result of the
	 *  handler's future (or an error if that future completes exceptionally).
	 *
	 * @param future future to compose the handler onto
	 * @param handler handler bi-function to compose onto the passed future
	 * @param <V> return type of original future
	 * @param <T> return type of final future
	 *
	 * @return future with same completion properties as the future returned by the handler
	 */
	public static <V, T> CompletableFuture<T> composeHandle(CompletableFuture<V> future, BiFunction<V,Throwable,? extends CompletableFuture<T>> handler) {
		return future.handle(handler).thenCompose(Function.identity());
	}

	/**
	 * Compose a handler bi-function to the result of a future. Unlike the
	 *  {@link CompletableFuture#handle(BiFunction) CompletableFuture.handle()}
	 *  function, which requires that the handler return a regular value, this
	 *  method requires that the handler return a {@link CompletableFuture}.
	 *  The returned future will then be ready with the result of the
	 *  handler's future (or an error if that future completes exceptionally).
	 *  The handler will execute on the {@link com.apple.foundationdb.FDB#DEFAULT_EXECUTOR default executor}
	 *  used for asynchronous tasks.
	 *
	 * @param future future to compose the handler onto
	 * @param handler handler bi-function to compose onto the passed future
	 * @param <V> return type of original future
	 * @param <T> return type of final future
	 *
	 * @return future with same completion properties as the future returned by the handler
	 */
	public static <V, T> CompletableFuture<T> composeHandleAsync(CompletableFuture<V> future, BiFunction<V,Throwable,? extends CompletableFuture<T>> handler) {
		return composeHandleAsync(future, handler, DEFAULT_EXECUTOR);
	}

	/**
	 * Compose a handler bi-function to the result of a future. Unlike the
	 *  {@link CompletableFuture#handle(BiFunction) CompletableFuture.handle()}
	 *  function, which requires that the handler return a regular value, this
	 *  method requires that the handler return a {@link CompletableFuture}.
	 *  The returned future will then be ready with the result of the
	 *  handler's future (or an error if that future completes excpetionally).
	 *  The handler will execute on the passed {@link Executor}.
	 *
	 * @param future future to compose the handler onto
	 * @param handler handler bi-function to compose onto the passed future
	 * @param executor executor on which to execute the handler function
	 * @param <V> return type of original future
	 * @param <T> return type of final future
	 *
	 * @return future with same completion properties as the future returned by the handler
	 */
	public static <V, T> CompletableFuture<T> composeHandleAsync(CompletableFuture<V> future, BiFunction<V,Throwable,? extends CompletableFuture<T>> handler, Executor executor) {
		return future.handleAsync(handler, executor).thenCompose(Function.identity());
	}

	/**
	 * Collects the results of many asynchronous processes into one asynchronous output. If
	 *  any of the tasks returns an error, the output is set to that error.
	 *
	 * @param tasks the tasks whose output is to be added to the output
	 * @param <V> return type of the asynchronous tasks
	 *
	 * @return a {@code CompletableFuture} that will be set to the collective result of the tasks
	 */
	public static <V> CompletableFuture<List<V>> getAll(final Collection<CompletableFuture<V>> tasks) {
		return whenAll(tasks).thenApply(unused -> {
			List<V> result = new ArrayList<>(tasks.size());
			for(CompletableFuture<V> f : tasks) {
				assert(f.isDone());
				result.add(f.getNow(null));
			}
			return result;
		});
	}

	/**
	 * Replaces the output of an asynchronous task with a predetermined value.
	 *
	 * @param task the asynchronous process whose output is to be replaced
	 * @param value the predetermined value to be returned on success of {@code task}
	 * @param <V> return type of original future
	 * @param <T> return type of final future
	 *
	 * @return a {@code CompletableFuture} that will be set to {@code value} on completion of {@code task}
	 */
	public static <V, T> CompletableFuture<V> tag(CompletableFuture<T> task, final V value) {
		return task.thenApply(o -> value);
	}

	/**
	 * Return a {@code CompletableFuture} that will be set when any of the {@link CompletableFuture}
	 *  inputs are done. A {@code CompletableFuture} is done both on success and failure.
	 *
	 * @param input the list of {@link CompletableFuture}s to monitor. This list
	 *  <b>must not</b> be modified during the execution of this call.
	 * @param <V> return type of the asynchronous tasks
	 *
	 * @return a signal that will be set when any of the {@code CompletableFuture}s are done
	 */
	public static <V> CompletableFuture<Void> whenAny(final Collection<? extends CompletableFuture<V>> input) {
		CompletableFuture<?>[] array = input.toArray(new CompletableFuture<?>[input.size()]);
		CompletableFuture<Object> anyOf = CompletableFuture.anyOf(array);
		return success(anyOf);
	}

	/**
	 * Return a {@code CompletableFuture} that will be set when all the {@link CompletableFuture}
	 *  inputs are done. A {@code CompletableFuture} is done both on success and failure.
	 *
	 * @param input the list of {@link CompletableFuture}s to monitor. This list
	 *  <b>must not</b> be modified during the execution of this call.
	 * @param <V> return type of the asynchronous tasks
	 *
	 * @return a signal that will be set when all of the {@code CompletableFuture}s are done
	 */
	public static <V> CompletableFuture<Void> whenAll(final Collection<? extends CompletableFuture<V>> input) {
		CompletableFuture<?>[] array = input.toArray(new CompletableFuture<?>[input.size()]);
		return CompletableFuture.allOf(array);
	}

	private AsyncUtil() {}
}
