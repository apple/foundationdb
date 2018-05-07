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

import java.util.concurrent.Executor;

class FutureResults extends NativeFuture<RangeResultInfo> {
	FutureResults(long cPtr, Executor executor) {
		super(cPtr);
		registerMarshalCallback(executor);
	}

	@Override
	protected void postMarshal() {
		// We can't close because this class actually marshals on-demand
	}

	@Override
	protected RangeResultInfo getIfDone_internal(long cPtr) throws FDBException {
		FDBException err = Future_getError(cPtr);

		if(!err.isSuccess()) {
			throw err;
		}

		return new RangeResultInfo(this);
	}

	public RangeResultSummary getSummary() {
		try {
			pointerReadLock.lock();
			return FutureResults_getSummary(getPtr());
		}
		finally {
			pointerReadLock.unlock();
		}
	}

	public RangeResult getResults() {
		try {
			pointerReadLock.lock();
			return FutureResults_get(getPtr());
		}
		finally {
			pointerReadLock.unlock();
		}
	}

	private native RangeResultSummary FutureResults_getSummary(long ptr) throws FDBException;
	private native RangeResult FutureResults_get(long cPtr) throws FDBException;
}
