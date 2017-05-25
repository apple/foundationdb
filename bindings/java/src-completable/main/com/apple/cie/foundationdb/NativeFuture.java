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

package com.apple.cie.foundationdb;

import java.util.concurrent.CompletableFuture;

abstract class NativeFuture<T> extends CompletableFuture<T> {
	protected final long cPtr;

	protected NativeFuture(long cPtr) {
		this.cPtr = cPtr;
	}

	// Adds a callback to call marshalWhenDone when the C-future
	// has completed. Every subclass should add this to its constructor
	// after it has initialized its members. This is excluded from the
	// constructor of this class because a quickly completing future can  
	// lead to a race where the marshalWhenDone tries to run on an
	// unconstructed subclass.
	protected void registerMarshalCallback() {
		Future_registerCallback(cPtr, new Runnable() {
			@Override
			public void run() {
				NativeFuture.this.marshalWhenDone();
			}
		});
	}

	private void marshalWhenDone() {
		try {
			T val = getIfDone_internal();
			postMarshal();
			complete(val);
		} catch(FDBException t) {
			assert(t.getCode() != 2015); // future_not_set not possible
			if(t.getCode() != 1102) { // future_released
				completeExceptionally(t);
			}
		} catch(Throwable t) {
			completeExceptionally(t);
		}
	}

	protected void postMarshal() {
		dispose();
	}

	abstract T getIfDone_internal() throws FDBException;

	public void dispose()  {
		Future_releaseMemory(cPtr);
	}

	@Override
	protected void finalize() throws Throwable {
		Future_dispose(cPtr);
	}

	@Override
	public T join() {
		Future_blockUntilReady(cPtr);
		return super.join();
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
