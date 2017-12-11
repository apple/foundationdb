/*
 * AsyncUtil.java
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
	public static final CompletableFuture<Void> DONE = CompletableFuture.completedFuture(null);
	public static final CompletableFuture<Boolean> READY_TRUE = CompletableFuture.completedFuture(true);
	public static final CompletableFuture<Boolean> READY_FALSE = CompletableFuture.completedFuture(false);

	/**
	 * Run {@code Function} {@code func}, returning all caught exceptions as a
	 *  {@code CompletableFuture} in an error state.
	 *
	 * @param func the {@code Function} to run
	 * @param value the input to pass to {@code func}
	 *
	 * @return the output of {@code func}, or a {@code CompletableFuture} carrying any exception
	 *  caught in the process.
	 */
	public static <I,O> CompletableFuture<O> applySafely( Function<I, ? extends CompletableFuture<O>> func, I value ) {
		try {
			return func.apply(value);
		} catch (RuntimeException e) {
			CompletableFuture<O> future = new CompletableFuture<>();
			future.completeExceptionally(e);
			return future;
		}
	}

	public static <V> CompletableFuture<Void> forEach(final AsyncIterable<V> iterable, final Consumer<? super V> consumer) {
		return forEach(iterable.iterator(), consumer);
	}

	public static <V> CompletableFuture<Void> forEach(final AsyncIterable<V> iterable, final Consumer<? super V> consumer, final Executor executor) {
		return forEach(iterable.iterator(), consumer, executor);
	}

	public static <V> CompletableFuture<Void> forEach(final AsyncIterator<V> iterator, final Consumer<? super V> consumer) {
		return forEach(iterator, consumer, DEFAULT_EXECUTOR);
	}

	public static <V> CompletableFuture<Void> forEach(final AsyncIterator<V> iterator, final Consumer<? super V> consumer, final Executor executor) {
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
	 * Iterates over a set of items and returns the result as a list.
	 *
	 * @param iterable the source of data over which to iterate
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collect(final AsyncIterable<V> iterable) {
		return collect(iterable, DEFAULT_EXECUTOR);
	}

	/**
	 * Iterates over a set of items and returns the result as a list.
	 *
	 * @param iterator the source of data over which to iterate. This function will exhaust the iterator.
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collect(final AsyncIterator<V> iterator) {
		return collect(iterator, DEFAULT_EXECUTOR);
	}

	/**
	 * Iterates over a set of items and returns the result as a list.
	 *
	 * @param iterable the source of data over which to iterate
	 * @param executor the {@link Executor} to use for asynchronous operations
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collect(final AsyncIterable<V> iterable, final Executor executor) {
		return collect(iterable.iterator(), executor);
	}

	/**
	 * Iterates over a set of items and returns the result as a list.
	 *
	 * @param iterator the source of data over which to iterate. This function will exhaust the iterator.
	 * @param executor the {@link Executor} to use for asynchronous operations
	 *
	 * @return a {@code CompletableFuture} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> CompletableFuture<List<V>> collect(final AsyncIterator<V> iterator, final Executor executor) {
		final List<V> accumulator = new LinkedList<>();
		return forEach(iterator, accumulator::add, executor).thenApply(ignore -> accumulator);
	}

	public static <V, T> AsyncIterable<T> mapIterable(final AsyncIterable<V> iterable, final Function<V, T> func) {
		return mapIterable(iterable, func, DEFAULT_EXECUTOR);
	}

	/**
	 * Map an {@code AsyncIterable} into an {@code AsyncIterable} of another type or with
	 *  each element modified in some fashion.
	 *
	 * @param iterable input
	 * @param func mapping function applied to each element
	 * @return a new iterable with each element mapped to a different value
	 */
	public static <V, T> AsyncIterable<T> mapIterable(final AsyncIterable<V> iterable,
			final Function<V, T> func, final Executor executor) {
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
	 * Map a {@code DisposableAsyncIterator} into a {@code DisposableAsyncIterator} of another type or with
	 *  each element modified in some fashion.
	 *
	 * @param iterator input
	 * @param func mapping function applied to each element
	 * @return a new iterator with each element mapped to a different value
	 */
	public static <V, T> DisposableAsyncIterator<T> mapIterator(final DisposableAsyncIterator<V> iterator,
													  final Function<V, T> func) {
		return new DisposableAsyncIterator<T>() {
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
			public void dispose() {
				iterator.dispose();
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
	 * @return a {@code PartialFuture} which will be set at completion of the loop.
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
	 * @return a {@code PartialFuture} which will be set at completion of the loop.
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
	 * @return a {@code PartialFuture} which will be set at completion of the loop.
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
	 * @return a {@code PartialFuture} which will be set at completion of the loop.
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
	 *
	 * @return a newly created {@code CompletableFuture} that is set when {@code task} completes
	 */
	public static <V> CompletableFuture<Void> success(CompletableFuture<V> task) {
		return task.thenApply(o -> null);
	}

	/**
	 * Maps the readiness of a {@link CompletableFuture} into a completion signal.  When
	 * the given {@link CompletableFuture} is set to a value or an error, the returned {@link CompletableFuture}
	 * will be set to null.  The returned {@link CompletableFuture} will never be set to an error unless
	 * it is explicitly cancelled.
	 *
	 * @param task the asynchronous process to monitor the readiness of
	 *
	 * @return a new {@link CompletableFuture} that is set when {@code task} is ready.
	 */
	public static <V> CompletableFuture<Void> whenReady(CompletableFuture<V> task) {
	    return task.handle((v, t) -> null);
	}

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

	public static <V, T> CompletableFuture<T> composeHandle(CompletableFuture<V> future, BiFunction<V,Throwable,? extends CompletableFuture<T>> fn) {
	    return future.handle(fn).thenCompose(Function.identity());
	}

	public static <V, T> CompletableFuture<T> composeHandleAsync(CompletableFuture<V> future, BiFunction<V,Throwable,? extends CompletableFuture<T>> fn) {
	    return composeHandleAsync(future, fn, DEFAULT_EXECUTOR);
	}

	public static <V, T> CompletableFuture<T> composeHandleAsync(CompletableFuture<V> future, BiFunction<V,Throwable,? extends CompletableFuture<T>> fn, Executor executor) {
		return future.handleAsync(fn, executor).thenCompose(Function.identity());
	}

	/**
	 * Collects the results of many asynchronous processes into one asynchronous output. If
	 *  any of the tasks returns an error, the output is set to that error.
	 *
	 * @param tasks the tasks whose output is to be added to the output
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
	 *
	 * @param value the predetermined value to be returned on success of {@code task}
	 *
	 * @return a {@code CompletableFuture} that will be set to {@code value} on completion of {@code task}
	 */
	public static <V, T> CompletableFuture<V> tag(CompletableFuture<T> task, final V value) {
		return task.thenApply(o -> value);
	}

	/**
	 * Return a {@code CompletableFuture} that will be set when any of the {@code PartialFuture}
	 *  inputs are done. A {@code CompletableFuture} is done both on success and failure.
	 *
	 * @param input the list of {@code PartialFuture}s to monitor. This list
	 *  <b>must not</b> be modified during the execution of this call.
	 *
	 * @return a signal that will be set when any of the {@code CompletableFuture}s are done
	 */
	public static <V> CompletableFuture<Void> whenAny(final Collection<? extends CompletableFuture<V>> input) {
		CompletableFuture<?>[] array = input.toArray(new CompletableFuture<?>[input.size()]);
		CompletableFuture<Object> anyOf = CompletableFuture.anyOf(array);
		return success(anyOf);
	}

	/**
	 * Return a {@code CompletableFuture} that will be set when all the {@code PartialFuture}
	 *  inputs are done. A {@code CompletableFuture} is done both on success and failure.
	 *
	 * @param input the list of {@code PartialFuture}s to monitor. This list
	 *  <b>must not</b> be modified during the execution of this call.
	 *
	 * @return a signal that will be set when all of the {@code CompletableFuture}s are done
	 */
	public static <V> CompletableFuture<Void> whenAll(final Collection<? extends CompletableFuture<V>> input) {
		CompletableFuture<?>[] array = input.toArray(new CompletableFuture<?>[input.size()]);
		return CompletableFuture.allOf(array);
	}

	private AsyncUtil() {}
}
