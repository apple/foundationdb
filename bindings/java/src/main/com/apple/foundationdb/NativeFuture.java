/*
 * NativeFuture.java
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

abstract class NativeFuture<T> extends CompletableFuture<T> implements AutoCloseable {
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	protected final Lock pointerReadLock = rwl.readLock();

	private long cPtr;

	protected NativeFuture(long cPtr) {
		this.cPtr = cPtr;
	}

	// Adds a callback to call marshalWhenDone when the C-future
	// has completed. Every subclass should add this to its constructor
	// after it has initialized its members. This is excluded from the
	// constructor of this class because a quickly completing future can  
	// lead to a race where the marshalWhenDone tries to run on an
	// unconstructed subclass.
	//
	// Since this must be called from a constructor, we assume that close
	// cannot be called concurrently.
	//
	// Note: This function guarantees the callback will be executed **at most once**.
	//
	protected void registerMarshalCallback(Executor executor) {
		if(cPtr != 0) {
			Future_registerCallback(cPtr, () -> executor.execute(this::marshalWhenDone));
		}
	}

	private void marshalWhenDone() {
		T val = null;
		try {
			boolean shouldComplete = false;
			try {
				pointerReadLock.lock();
				if(cPtr != 0) {
					val = getIfDone_internal(cPtr);
					shouldComplete = true;
				}
			}
			finally {
				pointerReadLock.unlock();
			}

			if(shouldComplete) {
				complete(val);
			}
		} catch(FDBException t) {
			assert(t.getCode() != 1102 && t.getCode() != 2015); // future_released, future_not_set not possible
			completeExceptionally(t);
		} catch(Throwable t) {
			completeExceptionally(t);
		} finally {
			postMarshal(val);
		}
	}

	protected void postMarshal(T value) {
		close();
	}

	protected abstract T getIfDone_internal(long cPtr) throws FDBException;

	@Override
	public void close() {
		long ptr = 0;

		rwl.writeLock().lock();
		if(cPtr != 0) {
			ptr = cPtr;
			cPtr = 0;
		}
		rwl.writeLock().unlock();

		if(ptr != 0) {
			Future_dispose(ptr);
			if(!isDone()) {
				completeExceptionally(new IllegalStateException("Future has been closed"));
			}
		}
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean result = super.cancel(mayInterruptIfRunning);
		try {
			rwl.readLock().lock();
			if(cPtr != 0) {
				Future_cancel(cPtr);
			}
			return result;
		}
		finally {
			rwl.readLock().unlock();
		}
	}

	protected long getPtr() {
		// we must have a read lock for this function to make sense, however it
		//  does not make sense to take the lock here, since the code that uses
		//  the result must inherently have the read lock itself.
		assert(rwl.getReadHoldCount() > 0);

		if (cPtr == 0) throw new IllegalStateException("Cannot access closed object");
		return cPtr;
	}

	private native void Future_registerCallback(long cPtr, Runnable callback);
	private native void Future_blockUntilReady(long cPtr);
	private native boolean Future_isReady(long cPtr);
	private native void Future_dispose(long cPtr);
	private native void Future_cancel(long cPtr);
	private native void Future_releaseMemory(long cPtr);

	// Used by FutureVoid
	protected native FDBException Future_getError(long cPtr);
}
