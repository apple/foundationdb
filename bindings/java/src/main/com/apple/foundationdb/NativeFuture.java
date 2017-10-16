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

import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.util.concurrent.Executor;

import com.apple.foundationdb.async.AbstractFuture;

abstract class NativeFuture<T> extends AbstractFuture<T> {
	protected final long cPtr;

	private final Object valueLock = new Object();
	private boolean valueSet = false;
	private T value;

	protected NativeFuture(long cPtr, Executor e) {
		super(e);
		this.cPtr = cPtr;
	}

	@Override
	protected void registerSingleCallback(Runnable callback) {
		if(isDone()) {
			callback.run();
		} else {
			Future_registerCallback(cPtr, callback);
		}
	}


	@Override
	public void blockInterruptibly() throws InterruptedException {
		if(this.isDone())
			return;
		blockChannel.blockUntilReady();
	}

	/**
	 * Blocks until this {@code Future} is set to either a value or an error.
	 * @throws FDBException
	 */
	@Override
	public void blockUntilReady() {
		NativeFuture.this.Future_blockUntilReady(cPtr);
	}

	/**
	 * Gets the readiness of this {@code Future}.  A {@code Future} is ready
	 *  if the value has been set or set to an error.
	 *
	 * @return <code>true</code> if the {@code Future} is set to a value or error
	 */
	@Override
	public boolean isDone() {
		return Future_isReady(cPtr);
	}

	@Override
	public boolean isError() {
		int code = Future_getError(cPtr).getCode();
		if(code==1102) throw new RuntimeException("");
		// is an error if is not success, future_not_set, or future_released
		return code != 0 && code != 2015 && code != 1102;
	}

	@Override
	public FDBException getError() {
		FDBException err = Future_getError(cPtr);

		// If this is not an error
		int code = err.getCode();
		if(code == 2015) // future_not_set
			throw new IllegalStateException("Future not ready");
		if(code == 1102 || code == 0) // future_released, success
			throw new IllegalStateException("Future set to value, not error");

		return err;
	}

	@Override
	protected T getIfDone() {
		try {
			T val = getIfDone_internal();
			synchronized (valueLock) {
				this.value = val;
				this.valueSet = true;
			}
			Future_releaseMemory(cPtr);
		} catch(FDBException t) {
			if(t.getCode() == 2015) { // future_not_set
				throw new IllegalStateException("Future not ready");
			}
			if(t.getCode() != 1102) { // future_released
				throw t;
			}
		}

		synchronized (valueLock) {
			// For us to get here with no value set that means that
			//  someone else called releaseMemory without also first
			//  setting "value". The only way this could happen is
			//  through a call to dispose().
			if(!valueSet)
				throw new IllegalStateException("Future value accessed after disposal");
			return this.value;
		}
	}

	abstract T getIfDone_internal() throws FDBException;

	@Override
	public void cancel() {
		Future_cancel(cPtr);
	}

	@Override
	public void dispose()  {
	    Future_releaseMemory(cPtr);
	    synchronized (valueLock) {
			this.value = null;
			this.valueSet = false;
		}
	}

	@Override
	protected void finalize() throws Throwable {
		Future_dispose(cPtr);
	}

	FutureChannel blockChannel = new FutureChannel();

	private native void Future_registerCallback(long cPtr, Runnable callback);
	private native void Future_blockUntilReady(long cPtr);
	private native boolean Future_isReady(long cPtr);
	private native void Future_dispose(long cPtr);
	private native void Future_cancel(long cPtr);
	private native void Future_releaseMemory(long cPtr);

	// Used by FutureVoid
	protected native FDBException Future_getError(long cPtr);

	private final class FutureChannel extends AbstractInterruptibleChannel {
		@Override
		protected void implCloseChannel() throws IOException {
			NativeFuture.this.cancel();
		}

		void blockUntilReady() throws InterruptedException {
			boolean completed = false;
			try {
				begin();
				NativeFuture.this.Future_blockUntilReady(cPtr);    // Perform blocking I/O operation
				completed = true;
			} finally {
				try {
					end(completed);
				} catch (AsynchronousCloseException e) {
					throw new InterruptedException();
				}
			}
		}
	}

}
