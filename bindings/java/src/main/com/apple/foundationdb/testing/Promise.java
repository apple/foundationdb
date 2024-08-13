/*
 * Promise.java
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

package com.apple.foundationdb.testing;

public class Promise {
	private long nativePromise;
	private boolean wasSet;
	private static native void send(long self, boolean value);
	private Promise(long nativePromise) {
		this.nativePromise = nativePromise;
		this.wasSet = false;
	}

	public boolean canBeSet() {
		return !wasSet;
	}

	public void send(boolean value) {
		if (wasSet) {
			throw new IllegalStateException("Promise was already set");
		}
		wasSet = true;
		send(nativePromise, value);
	}

}
