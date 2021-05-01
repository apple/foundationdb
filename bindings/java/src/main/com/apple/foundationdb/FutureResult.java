/*
 * FutureResult.java
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

import com.apple.foundationdb.EventKeeper.Events;

class FutureResult extends NativeFuture<byte[]> {
	private final EventKeeper eventKeeper;

	FutureResult(long cPtr, Executor executor, EventKeeper eventKeeper) {
		super(cPtr);
		this.eventKeeper = eventKeeper;
		registerMarshalCallback(executor);
	}

	@Override
	protected byte[] getIfDone_internal(long cPtr) throws FDBException {
		return FutureResult_get(cPtr);
	}

	@Override
	protected void postMarshal(byte[] value){
		if(value!=null && eventKeeper!=null){
			eventKeeper.count(Events.BYTES_FETCHED, value.length);
		}
		super.postMarshal(value);
	}

	private native byte[] FutureResult_get(long cPtr) throws FDBException;
}
