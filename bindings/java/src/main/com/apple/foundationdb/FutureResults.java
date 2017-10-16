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

package com.apple.cie.foundationdb;

import java.util.concurrent.Executor;

class FutureResults extends NativeFuture<RangeResult> {
	FutureResults(long cPtr, Executor e) {
		super(cPtr, e);
	}

	@Override
	public RangeResult getIfDone_internal() throws FDBException {
		return FutureResults_get(cPtr);
	}

	public RangeResultSummary getSummaryIfDone() throws FDBException {
		return FutureResults_getSummary(cPtr);
	}

	private native RangeResultSummary FutureResults_getSummary(long ptr) throws FDBException;
	private native RangeResult FutureResults_get(long cPtr) throws FDBException;
}
