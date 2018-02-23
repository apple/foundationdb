/*
 * FutureDatabase.java
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

class FutureDatabase extends NativeFuture<Database> {
	private final Executor executor;

	FutureDatabase(long cPtr, Executor executor) {
		super(cPtr);
		this.executor = executor;
		registerMarshalCallback(executor);
	}

	@Override
	protected Database getIfDone_internal(long cPtr) throws FDBException {
		return new FDBDatabase(FutureDatabase_get(cPtr), executor);
	}

	private native long FutureDatabase_get(long cPtr) throws FDBException;
}
