/*
 * NativeObjectWrapper.java
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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

abstract class NativeObjectWrapper implements AutoCloseable {
	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	protected final Lock pointerReadLock = rwl.readLock();

	private boolean closed = false;
	private long cPtr;

	NativeObjectWrapper(long cPtr) {
		this.cPtr = cPtr;
		if(this.cPtr == 0)
			this.closed = true;
	}

	public boolean isClosed() {
		// we must have a read lock for this function to make sense, however it
		//  does not make sense to take the lock here, since the code that uses
		//  the result must inherently have the read lock itself.
		assert(rwl.getReadHoldCount() > 0);

		return closed;
	}

	public void checkUnclosed(String context) {
		try {
			if(FDB.instance().warnOnUnclosed && !closed) {
				System.err.println(context + " not closed");
			}
		}
		catch(Exception e) {
			// Eat this error. This is called from the finalizer,
			// so there isn't much we can do.
		}
	}

	@Override
	public void close() {
		rwl.writeLock().lock();
		long ptr = 0;
		try {
			if(closed)
				return;

			ptr = cPtr;
			this.cPtr = 0;
			closed = true;
		} finally {
			rwl.writeLock().unlock();
		}

		closeInternal(ptr);
	}

	protected long getPtr() {
		// we must have a read lock for this function to make sense, however it
		//  does not make sense to take the lock here, since the code that uses
		//  the result must inherently have the read lock itself.
		assert(rwl.getReadHoldCount() > 0);

		if(this.closed)
			throw new IllegalStateException("Cannot access closed object");

		return this.cPtr;
	}

	protected abstract void closeInternal(long cPtr);
}
