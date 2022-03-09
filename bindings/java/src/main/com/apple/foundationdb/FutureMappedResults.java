/*
 * FutureMappedResults.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.EventKeeper.Events;

class FutureMappedResults extends NativeFuture<MappedRangeResultInfo> {
	private final EventKeeper eventKeeper;
	FutureMappedResults(long cPtr, boolean enableDirectBufferQueries, Executor executor, EventKeeper eventKeeper) {
		super(cPtr);
		registerMarshalCallback(executor);
		this.enableDirectBufferQueries = enableDirectBufferQueries;
		this.eventKeeper = eventKeeper;
	}

	@Override
	protected void postMarshal(MappedRangeResultInfo rri) {
		// We can't close because this class actually marshals on-demand
	}

	@Override
	protected MappedRangeResultInfo getIfDone_internal(long cPtr) throws FDBException {
		if (eventKeeper != null) {
			eventKeeper.increment(Events.JNI_CALL);
		}
		FDBException err = Future_getError(cPtr);

		if (err != null && !err.isSuccess()) {
			throw err;
		}

		return new MappedRangeResultInfo(this);
	}

	public MappedRangeResult getResults() {
		ByteBuffer buffer = enableDirectBufferQueries ? DirectBufferPool.getInstance().poll() : null;
		if (buffer != null && eventKeeper != null) {
			eventKeeper.increment(Events.RANGE_QUERY_DIRECT_BUFFER_HIT);
			eventKeeper.increment(Events.JNI_CALL);
		} else if (eventKeeper != null) {
			eventKeeper.increment(Events.RANGE_QUERY_DIRECT_BUFFER_MISS);
			eventKeeper.increment(Events.JNI_CALL);
		}

		try {
			pointerReadLock.lock();
			if (buffer != null) {
				try (MappedRangeResultDirectBufferIterator directIterator =
				         new MappedRangeResultDirectBufferIterator(buffer)) {
					FutureMappedResults_getDirect(getPtr(), directIterator.getBuffer(),
					                              directIterator.getBuffer().capacity());
					return new MappedRangeResult(directIterator);
				}
			} else {
				return FutureMappedResults_get(getPtr());
			}
		} finally {
			pointerReadLock.unlock();
		}
	}

	private boolean enableDirectBufferQueries = false;

	private native MappedRangeResult FutureMappedResults_get(long cPtr) throws FDBException;
	private native void FutureMappedResults_getDirect(long cPtr, ByteBuffer buffer, int capacity) throws FDBException;
}
