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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provided utilities for using and manipulating {@link Future}s. Many of the methods
 *  in this class have two versions -- one for dealing with {@link PartialFuture}s and
 *  one for {@code Future}s.
 */
public class AsyncUtil {
	/**
	 * Run {@code Function} {@code func}, returning all caught exceptions as a
	 *  {@code Future} in an error state.
	 *
	 * @param func the {@code Function} to run
	 * @param value the input to pass to {@code func}
	 *
	 * @return the output of {@code func}, or a {@code Future} carrying any exception
	 *  caught in the process.
	 */
	public static <I,O> Future<O> applySafely( Function<I,Future<O>> func, I value ) {
		try {
			return func.apply(value);
		} catch (RuntimeException e) {
			return new ReadyFuture<O>(e);
		}
	}

	/**
	 * Run {@code PartialFunction} {@code func}, returning all caught exceptions as a
	 *  {@code PartialFuture} in an error state.
	 *
	 * @param func the {@code PartialFunction} to run
	 * @param value the input to pass to {@code func}
	 *
	 * @return the output of {@code func}, or a {@code PartialFuture} carrying any exception
	 *  caught in the process.
	 */
	public static <I,O> PartialFuture<O> applySafely( PartialFunction<I,? extends PartialFuture<O>> func, I value ) {
		try {
			return func.apply(value);
		} catch (Exception e) {
			return new ReadyPartialFuture<O>(e);
		}
	}

	/**
	 * Iterates over a set of items and returns the result as a list.
	 *
	 * @param iterable the source of data over which to iterate
	 *
	 * @return a {@code Future} which will be set to the amalgamation of results
	 *  from iteration.
	 */
	public static <V> Future<List<V>> collect(final AsyncIterable<V> iterable) {
		final AsyncIterator<V> it = iterable.iterator();
		final List<V> accumulator = new LinkedList<V>();

		// The condition of the while loop is simply "onHasNext()" returning true
		Function<Void, Future<Boolean>> condition = new Function<Void, Future<Boolean>>() {
			@Override
			public Future<Boolean> apply(Void v) {
				return it.onHasNext().map(new Function<Boolean, Boolean>() {
					@Override
					public Boolean apply(Boolean o) {
						if(o) {
							accumulator.add(it.next());
						}
						return o;
					}
				});
			}
		};

		Future<Void> complete = whileTrue(condition);
		Future<List<V>> result = tag(complete, accumulator);

		return result;
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
			final Function<V, T> func) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> iterator() {
				final AsyncIterator<V> it = iterable.iterator();
				return new AsyncIterator<T>() {

					@Override
					public void remove() {
						it.remove();
					}

					@Override
					public Future<Boolean> onHasNext() {
						return it.onHasNext();
					}

					@Override
					public boolean hasNext() {
						return it.hasNext();
					}

					@Override
					public T next() {
						return func.apply(it.next());
					}

					@Override
					public void cancel() {
						it.cancel();
					}

					@Override
					public void dispose() {
						it.dispose();
					}
				};
			}

			@Override
			public Future<List<T>> asList() {
				return iterable.asList().map(new Function<List<V>, List<T>>() {
					@Override
					public List<T> apply(List<V> o) {
						ArrayList<T> out = new ArrayList<T>(o.size());
						for(V in : o)
							out.add(func.apply(in));
						return out;
					}
				});
			}
		};
	}

	/**
	 * Cast a checked {@link Exception} in a {@code RuntimeException} for when code
	 *  knows that a {@code PartialFuture} is an instance of a {@code Future}.
	 */
	private static Function<Exception, Future<Void>> makeTotal = new Function<Exception, Future<Void>>() {
		@Override
		public Future<Void> apply(Exception e) {
			throw (RuntimeException)e;
		}
	};

	private static class LoopPartial {
		final PartialFunction<Void, ? extends PartialFuture<Boolean>> body;
		final SettablePartialFuture<Void> done;
		PartialFuture<Boolean> process;
		boolean m_cancelled = false;

		public LoopPartial(PartialFunction<Void, ? extends PartialFuture<Boolean>> body) {
			this.body = body;
			this.done = new SettablePartialFuture<Void>();
			this.done.onCancelled(new Runnable() {
				@Override
				public void run() {
					synchronized( LoopPartial.this ) {
						LoopPartial.this.m_cancelled = true;
						LoopPartial.this.process.cancel();
					}
				}
			});
			this.run();
		}

		private boolean shouldContinue(PartialFuture<Boolean> process) {
			try {
				// Any exception encountered on process will be re-thrown here
				if(process.get())
					return true;
			} catch (Exception e) {
				done.setError(e);
				return false;
			} catch (Error e) {
				done.setError(e);
				throw e;
			}
			done.set(null);
			return false;
		}

		private void run() {
			while(true) {
				try {
					final PartialFuture<Boolean> process = body.apply(null);
					synchronized (this) {
						if (m_cancelled) process.cancel();
						this.process = process;
					}

					if(process.onReadyAlready(new Runnable() {
						@Override
						public void run() {
							if(shouldContinue(process))
								LoopPartial.this.run();
						}}))
					{
						if(shouldContinue(process))
							continue;
					}
				} catch(Exception e) {
					done.setError(e);
				} catch (Error e) {
					done.setError(e);
					throw e;
				}

				break;
			};
		}
	}

	/**
	 * Executes an asynchronous operation repeatedly until it returns {@code False}.
	 *
	 * @param body the asynchronous operation over which to loop
	 *
	 * @return a {@code PartialFuture} which will be set at completion of the loop.
	 */
	public static PartialFuture<Void> whileTrue(PartialFunction<Void,? extends PartialFuture<Boolean>> body) {
		return new LoopPartial(body).done;
	}

	/**
	 * Executes an asynchronous operation repeatedly until it returns {@code False}.
	 *
	 * @param body the asynchronous operation over which to loop
	 *
	 * @return a {@code Future} which will be set at completion of the loop.
	 */
	public static Future<Void> whileTrue(Function<Void,Future<Boolean>> body) {
		// Since body can't throw checked exceptions and the implementation of LoopPartial
		// doesn't create any, we can use the partial version of the while loop and then
		// simply force any resulting Exceptions to type RuntimeException
		return new LoopPartial(body).done.rescue(makeTotal);
	}

	/**
	 * Executes {@code body} repeatedly after each time {@code condition} returns {@code true}.
	 * At each iteration, {@code condition} is evaluated <i>before</i> the call to {@code body}.
	 *
	 * @param condition evaluated at the start of each loop, if it returns {@code true},
	 *  {@code body} is called.
	 * @param body called each time that {@code condition} returns {@code true}.
	 *
	 * @return a signal set when the condition finally returns {@code false} and the loop is
	 *  complete.
	 */
	/*public static Future<Void> whileTrue(
			final Callable<Future<Boolean>> condition,
			final Callable<Future<Void>> body )
	{
// This WOULD be the lambda version of this function, but we're on Java 6!
//		return While( () ->
//			condition.apply(null).flatMap( cond_true ->
//				cond_true ? tag(body.apply(null), true) : new Settable<Boolean>(false)
//					) );

		return whileTrue(new Callable<Future<Boolean>>() {
			@Override
			public Future<Boolean> call() {
				try {
					return condition.call().flatMap(new Function<Boolean, Future<Boolean>>() {
						@Override
						public Future<Boolean> apply(Boolean cond_true) throws Exception {
							if(cond_true) {
								return tag(body.call(), Boolean.TRUE);
							} else {
								return new ReadyPartialFuture<Boolean>(false);
							}
						}});
				} catch (Throwable e) {
					return new ReadyPartialFuture<Boolean>(e);
				}
			}
		});
	}*/

	/**
	 * Maps the outcome of a task into a completion signal. Can be useful if {@code task} has
	 *  side-effects for which all is needed is a signal of completion.
	 *  All errors from {@code task} will be passed to the resulting {@code Future}.
	 *
	 * @param task the asynchronous process for which to signal completion
	 *
	 * @return a newly created {@code Future} that is set when {@code task} completes
	 */
	public static <V> Future<Void> success(Future<V> task) {
		return task.map(new Function<V, Void>() {
			@Override
			public Void apply(V o) {
				return null;
			}
		});
	}

	/**
	 * Maps the outcome of a task into a completion signal. Can be useful if {@code task} has
	 *  side-effects for which all is needed is a signal of completion.
	 *  All errors from {@code task} will be passed to the resulting {@code PartialFuture}.
	 *
	 * @param task the asynchronous process for which to signal completion
	 *
	 * @return a newly created {@code Future} that is set when {@code task} completes
	 */
	public static <V> PartialFuture<Void> success(PartialFuture<V> task) {
		return task.map(new Function<V, Void>() {
			@Override
			public Void apply(V o) {
				return null;
			}
		});
	}

	/**
	 * Maps the readiness of a {@link PartialFuture} into a completion signal.  When
	 * the given {@link PartialFuture} is set to a value or an error, the returned {@link Future}
	 * will be set to null.  The returned {@link Future} will never be set to an error unless
	 * it is explicitly cancelled.
	 *
	 * @param task the asynchronous process to monitor the readiness of
	 *
	 * @return a new {@link Future} that is set when {@code task} is ready.
	 */
	public static <V> Future<Void> whenReady(PartialFuture<V> task) {
		return task.map(new PartialFunction<V, Void>() {
			@Override
			public Void apply(V o) throws Exception {
				return null;
			}
		}).rescue(new Function<Exception, Future<Void>>() {
			@Override
			public Future<Void> apply(Exception o) {
				return ReadyFuture.DONE;
			}
		});
	}

	/**
	 * Collects the results of many asynchronous processes into one asynchronous output. If
	 *  any of the tasks returns an error, the output is set to that error.
	 *
	 * @param tasks the tasks whose output is to be added to the output
	 *
	 * @return a {@code Future} that will be set to the collective result of the tasks
	 */
	public static <V> Future<List<V>> getAll(final Collection<Future<V>> tasks) {
		return whenAll(tasks).map(new Function<Void, List<V>>() {
			@Override
			public List<V> apply(Void o) {
				List<V> result = new ArrayList<V>();
				for(Future<V> f : tasks)
					result.add(f.get());
				return result;
			}
		});
	}

	/**
	 * Collects the results of many asynchronous processes into one asynchronous output. If
	 *  any of the tasks returns an error, the output is set to that error.
	 *
	 * @param tasks the tasks whose output is to be added to the output
	 *
	 * @return a {@code Future} that will be set to the collective result of the tasks
	 */
	public static <V> PartialFuture<List<V>> getAllPartial(final Collection<PartialFuture<V>> tasks) {
		return whenAll(tasks).map(new PartialFunction<Void, List<V>>() {
			@Override
			public List<V> apply(Void o) throws Exception {
				List<V> result = new ArrayList<V>();
				for(PartialFuture<V> f : tasks)
					result.add( f.get() );
				return result;
			}
		});
	}

	/**
	 * Replaces the output of an asynchronous task with a predetermined value.
	 *
	 * @param task the asynchronous process whose output is to be replaced
	 *
	 * @param value the predetermined value to be returned on success of {@code task}
	 *
	 * @return a {@code Future} that will be set to {@code value} on completion of {@code task}
	 */
	public static <V, T> Future<V> tag(Future<T> task, final V value) {
		return task.map(new Function<T, V>() {
			@Override
			public V apply(T o) {
				return value;
			}
		});
	}

	/**
	 * Replaces the output of an asynchronous task with a predetermined value.
	 *
	 * @param task the asynchronous process whose output is to be replaced
	 *
	 * @param value the predetermined value to be returned on success of {@code task}
	 *
	 * @return a {@code Future} that will be set to {@code value} on completion of {@code task}
	 */
	public static <V, T> PartialFuture<V> tag(PartialFuture<T> task, final V value) {
		return task.map(new Function<T, V>() {
			@Override
			public V apply(T o) {
				return value;
			}
		});
	}

	/**
	 * Return a {@code Future} that will be set when any of the {@code PartialFuture}
	 *  inputs are done. A {@code Future} is done both on success and failure.
	 *
	 * @param input the list of {@code PartialFuture}s to monitor. This list
	 *  <b>must not</b> be modified during the execution of this call.
	 *
	 * @return a signal that will be set when any of the {@code Future}s are done
	 */
	public static <V> Future<Void> whenAny(final Collection<? extends PartialFuture<V>> input) {
		// Short-circuit work for the case that there is no need for the callback mechanism
		for(PartialFuture<?> a : input) {
			if(a.isDone()) {
				return ReadyFuture.DONE;
			}
		}

		final SettableFuture<Void> p = new SettableFuture<Void>();
		for(PartialFuture<?> a : input) {
			p.onCancelledCancel(a);
			a.onReady(new Runnable() {
				@Override
				public void run() {
					if(p.isSet())
						return;
					try {
						p.set((Void)null);
					} catch(Exception e) {
						// There is significant chance that this is a race to set the
						//  promise, therefore this being an error is not surprising.
						//  Also, there is nowhere for an exception to go, aside from
						//  from going to a default handler.
					}
				}
			});
		}
		return p;
	}

	/**
	 * Return a {@code Future} that will be set when all the {@code PartialFuture}
	 *  inputs are done. A {@code Future} is done both on success and failure.
	 *
	 * @param input the list of {@code PartialFuture}s to monitor. This list
	 *  <b>must not</b> be modified during the execution of this call.
	 *
	 * @return a signal that will be set when all of the {@code Future}s are done
	 */
	public static <V> Future<Void> whenAll(final Collection<? extends PartialFuture<V>> input) {
		int count = input.size();
		if(count == 0) {
			return ReadyFuture.DONE;
		}

		// Is this possibly susceptible to a race where the count is taken before
		//  the iteration and therefore the collection could have been modified?
		//  This could lead to a mismatch in the
		final AtomicInteger outstanding = new AtomicInteger(count);
		final SettableFuture<Void> p = new SettableFuture<Void>();
		for(PartialFuture<?> a : input) {
			p.onCancelledCancel(a);
			a.onReady(new Runnable() {
				@Override
				public void run() {
					if(outstanding.decrementAndGet() == 0) {
						p.set((Void)null);
					}
				}
			});
		}
		return p;
	}

	private AsyncUtil() {}

	/*
	private static <T> AsyncIterable<T> readyIterable(final Iterable<T> it) {
		return new AsyncIterable<T>() {
			@Override
			public AsyncIterator<T> iterator() {
				return new ReadyIterator<T>(it.iterator());
			}

			@Override
			public Future<List<T>> asList() {
				ArrayList<T> c = new ArrayList<T>();
				for(T t: it) {
					c.add(t);
				}
				return new ReadyFuture<List<T>>(c);
			}
		};
	}

	private static class ReadyIterator<T> implements AsyncIterator<T> {
		private Iterator<T> iterator;

		public ReadyIterator(Iterator<T> iterator) {
			this.iterator = iterator;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public T next() {
			return iterator.next();
		}

		@Override
		public void remove() {
			// This will throw if the underlying container does not support it
			iterator.remove();
		}

		@Override
		public Future<Boolean> onHasNext() {
			return new ReadyFuture<Boolean>(iterator.hasNext());
		}
	}

	private static void testCollectionAction(AsyncIterable<Integer> readyIterable) {
		Future<List<Integer>> collected = collect(readyIterable);
		try {
			List<Integer> list = collected.get();
			System.out.println("Via dual parameter (default) collect(): List of " + list.size() + " items");
		} catch(Throwable t) {
			t.printStackTrace();
		}

		collected = singleBodyTest(readyIterable);
		try {
			List<Integer> list = collected.get();
			System.out.println("Via single parmeter (test) collections: List of " + list.size() + " items");
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// test collect over large set of items
		AsyncIterable<Integer> readyIterable = readyIterable(Collections.nCopies(10000, Integer.valueOf(14)));
		for(int i = 0; i < 100; i++) {
			testCollectionAction(readyIterable);
		}
	}*/
}
