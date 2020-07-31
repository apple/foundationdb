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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

class FutureResults {
	FutureResults(CompletableFuture<RangeResult> f) {
		future = f;
	}

	public CompletableFuture<RangeResult> getFuture() {
		return future;
	}

	public void cancel(boolean b) {
		// TODO (Vishesh): How to tell JNI to cancel and dispose internal C FDBFuture?
		future.cancel(b);
	};

	public RangeResultSummary getSummary() throws InterruptedException, ExecutionException {
		return getResults().getSummary();
	}

	public RangeResult getResults() throws InterruptedException, ExecutionException {
		assert(future.isDone()); /* Impossible */
		return future.get();
	}

	final private CompletableFuture<RangeResult> future;
}
