/*
 * FutureResults.java
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

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

class FutureResults extends NativeFuture<RangeResultInfo> {
	FutureResults(long cPtr, boolean enableDirectBufferQueries, Executor executor) {
		super(cPtr);
		registerMarshalCallback(executor);
		this.enableDirectBufferQueries = enableDirectBufferQueries;
	}

	@Override
	protected void postMarshal() {
		// We can't close because this class actually marshals on-demand
	}

	@Override
	protected RangeResultInfo getIfDone_internal(long cPtr) throws FDBException {
		FDBException err = Future_getError(cPtr);

		if(err != null && !err.isSuccess()) {
			throw err;
		}

		return new RangeResultInfo(this);
	}

	public RangeResult getResults() {
		ByteBuffer buffer = enableDirectBufferQueries
			? DirectBufferPool.getInstance().poll()
			: null;
		try {
			pointerReadLock.lock();
			if (buffer != null) {
				try (DirectBufferIterator directIterator = new DirectBufferIterator(buffer)) {
					FutureResults_getDirect(getPtr(), directIterator.getBuffer(), directIterator.getBuffer().capacity());
					return new RangeResult(directIterator);
				}
			} else {
				return FutureResults_get(getPtr());
			}
		} finally {
			pointerReadLock.unlock();
		}
	}

	private boolean enableDirectBufferQueries = false;

	private native RangeResult FutureResults_get(long cPtr) throws FDBException;
	private native void FutureResults_getDirect(long cPtr, ByteBuffer buffer, int capacity)
		throws FDBException;
}
